#!/usr/bin/env python3
"""
Monitor EcoFlow (Delta 2 + batería extra) → informe por Telegram.

Un solo proceso, corre 24/7 en Railway: escucha comandos de Telegram al
instante (long polling) y en paralelo chequea cada cierto tiempo si toca
mandar el informe periódico o si empezó a cargar por corriente. Todo el
estado (pausa, última carga AC) vive en memoria mientras el proceso corre.

Variables de entorno requeridas:
  ECOFLOW_ACCESS_KEY   Access key de developer.ecoflow.com
  ECOFLOW_SECRET_KEY   Secret key de developer.ecoflow.com
  ECOFLOW_SN_DELTA2    Número de serie de la Delta 2
  ECOFLOW_SN_EXTRA     Número de serie de la batería extra (opcional)
  TELEGRAM_BOT_TOKEN   Token del bot (de @BotFather)
  TELEGRAM_CHAT_ID     Chat ID destino (de @userinfobot)
  INTERVAL_HOURS       Intervalo del informe periódico en horas (default: 1)
  AC_CHECK_MINUTES     Cada cuánto chequear si empezó a cargar por AC (default: 1)

Uso:
  python3 ecoflow_telegram_monitor.py
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import random
import sys
import threading
import time
import uuid as uuid_lib
from datetime import datetime

import requests

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

API_HOST = os.environ.get("ECOFLOW_API_HOST", "https://api.ecoflow.com")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ecoflow-monitor")


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        log.error("Falta la variable de entorno %s", name)
        sys.exit(1)
    return value


# Las credenciales de EcoFlow son opcionales al arrancar: el bot debe poder
# responder /start y /help aunque todavía no estén listas (p. ej. mientras
# se espera la aprobación del programa developer o el permiso del dispositivo).
ACCESS_KEY = os.environ.get("ECOFLOW_ACCESS_KEY", "").strip()
SECRET_KEY = os.environ.get("ECOFLOW_SECRET_KEY", "").strip()
SN_DELTA2 = os.environ.get("ECOFLOW_SN_DELTA2", "").strip()
SN_EXTRA = os.environ.get("ECOFLOW_SN_EXTRA", "").strip()
BOT_TOKEN = require_env("TELEGRAM_BOT_TOKEN")
CHAT_ID = require_env("TELEGRAM_CHAT_ID")
INTERVAL_HOURS = float(os.environ.get("INTERVAL_HOURS", "1"))
AC_CHECK_MINUTES = float(os.environ.get("AC_CHECK_MINUTES", "1"))
AC_WATTS_THRESHOLD = 5  # por debajo de esto se considera "no está cargando por AC"

# Modo "private API": en vez de las developer keys (bloqueadas por IP en
# Railway y sin permiso de dispositivo todavía), inicia sesión con el email y
# contraseña normales de la cuenta EcoFlow y habla por MQTT — el mismo canal
# que usa la app móvil. No oficial/ingeniería inversa, pero no pasa por el
# developer portal para nada, así que evita los dos bloqueos que tuvimos ahí.
ECOFLOW_EMAIL = os.environ.get("ECOFLOW_EMAIL", "").strip()
ECOFLOW_PASSWORD = os.environ.get("ECOFLOW_PASSWORD", "").strip()
USE_PRIVATE_API = bool(ECOFLOW_EMAIL and ECOFLOW_PASSWORD)

ECOFLOW_READY = bool(SN_DELTA2 and (USE_PRIVATE_API or (ACCESS_KEY and SECRET_KEY)))

if not ECOFLOW_READY:
    log.warning("EcoFlow no configurado todavía (faltan credenciales o el serial de la Delta 2)")
elif USE_PRIVATE_API:
    log.info("Usando el modo 'private API' (MQTT con email/password) en vez de las developer keys")

START_TIME = time.time()
AUTO_PAUSED = False
WAS_CHARGING_AC = False

# --- Estado del cliente MQTT privado (solo si USE_PRIVATE_API) ---
_mqtt_client = None
_mqtt_user_id = None
_device_cache = {}  # sn -> {"quota": {...}, "online": bool, "updated_at": ts}
_mqtt_cache_lock = threading.Lock()


def _flatten(obj, prefix=""):
    """Aplana dicts/listas al formato clave.subclave / clave[0] que exige la firma de EcoFlow."""
    items = {}
    if isinstance(obj, dict):
        for k in sorted(obj.keys()):
            key = f"{prefix}.{k}" if prefix else k
            items.update(_flatten(obj[k], key))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            items.update(_flatten(v, f"{prefix}[{i}]"))
    else:
        items[prefix] = obj
    return items


def _signed_headers(params: dict) -> dict:
    nonce = str(random.randint(100000, 999999))
    timestamp = str(int(time.time() * 1000))
    flat = _flatten(params) if params else {}
    parts = [f"{k}={v}" for k, v in sorted(flat.items())]
    parts += [f"accessKey={ACCESS_KEY}", f"nonce={nonce}", f"timestamp={timestamp}"]
    sign_str = "&".join(parts)
    signature = hmac.new(SECRET_KEY.encode("utf-8"), sign_str.encode("utf-8"), hashlib.sha256).hexdigest()
    return {
        "accessKey": ACCESS_KEY,
        "nonce": nonce,
        "timestamp": timestamp,
        "sign": signature,
        "Content-Type": "application/json;charset=UTF-8",
    }


# EcoFlow bloquea las IPs de datacenter de Railway (confirmado: code 8513
# "accessKey is invalid" solo desde ahí, la misma key funciona bien desde
# otras IPs). Como solución gratuita, se prueban proxies públicos hasta
# encontrar uno que no esté bloqueado, y se recuerda cuál funcionó para no
# tener que probar la lista entera en cada llamada. Los proxies gratuitos
# se caen seguido, por eso hay reintento con rotación en cada request.
USE_PROXY = os.environ.get("ECOFLOW_USE_PROXY", "true").lower() != "false"
PROXY_LIST_URL = (
    "https://api.proxyscrape.com/v4/free-proxy-list/get"
    "?request=display_proxies&proxy_format=protocolipport&format=text&protocol=http"
)
_proxy_pool = []
_proxy_pool_fetched_at = 0
_last_good_proxy = None


def _refresh_proxy_pool() -> list:
    global _proxy_pool, _proxy_pool_fetched_at
    if _proxy_pool and time.time() - _proxy_pool_fetched_at < 1800:
        return _proxy_pool
    try:
        resp = requests.get(PROXY_LIST_URL, timeout=10)
        resp.raise_for_status()
        proxies = [line.strip() for line in resp.text.splitlines() if line.strip()]
        random.shuffle(proxies)
        _proxy_pool = proxies
        _proxy_pool_fetched_at = time.time()
        log.info("Lista de proxies renovada: %d candidatos", len(proxies))
    except Exception:
        log.exception("No se pudo renovar la lista de proxies")
    return _proxy_pool


def _ecoflow_get(path: str, params: dict) -> dict:
    """GET firmado a la API de EcoFlow, probando proxies hasta encontrar uno que no esté bloqueado."""
    global _last_good_proxy

    def _try(proxy_url):
        headers = _signed_headers(params)
        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
        resp = requests.get(f"{API_HOST}{path}", params=params, headers=headers, proxies=proxies, timeout=8)
        resp.raise_for_status()
        payload = resp.json()
        code = str(payload.get("code"))
        if code == "8513":  # esta IP/proxy está bloqueada; hay que probar otra
            raise ConnectionError("IP bloqueada por EcoFlow (8513)")
        if code != "0":
            raise RuntimeError(f"EcoFlow API error: {payload}")
        return payload

    if not USE_PROXY:
        return _try(None)

    candidates = []
    if _last_good_proxy:
        candidates.append(_last_good_proxy)
    candidates += [p for p in _refresh_proxy_pool() if p != _last_good_proxy][:8]

    last_exc = None
    for proxy_url in candidates:
        try:
            payload = _try(proxy_url)
            _last_good_proxy = proxy_url
            return payload
        except Exception as exc:
            last_exc = exc
            continue
    raise RuntimeError(f"Ningún proxy funcionó (probados {len(candidates)}): {last_exc}")


def _mqtt_topics(sn: str) -> dict:
    return {
        "get": f"/app/{_mqtt_user_id}/{sn}/thing/property/get",
        "get_reply": f"/app/{_mqtt_user_id}/{sn}/thing/property/get_reply",
        "data": f"/app/device/property/{sn}",
    }


def _private_login() -> tuple:
    resp = requests.post(
        f"{API_HOST}/auth/login",
        headers={"lang": "en_US", "content-type": "application/json"},
        json={
            "email": ECOFLOW_EMAIL,
            "password": base64.b64encode(ECOFLOW_PASSWORD.encode()).decode(),
            "scene": "IOT_APP",
            "userType": "ECOFLOW",
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    if str(data.get("message", "")).lower() != "success":
        raise RuntimeError(f"Login EcoFlow (private API) falló: {data}")
    return data["data"]["token"], data["data"]["user"]["userId"]


def _private_mqtt_creds(token: str) -> tuple:
    resp = requests.get(
        f"{API_HOST}/iot-auth/app/certification",
        headers={"lang": "en_US", "authorization": f"Bearer {token}", "content-type": "application/json"},
        timeout=15,
    )
    resp.raise_for_status()
    d = resp.json()["data"]
    return d["url"], int(d["port"]), d["certificateAccount"], d["certificatePassword"]


def _request_quota_refresh(sn: str) -> None:
    if _mqtt_client is None:
        return
    topics = _mqtt_topics(sn)
    payload = json.dumps({"version": "1.1", "moduleType": 0, "operateType": "latestQuotas", "params": {}})
    _mqtt_client.publish(topics["get"], payload)


def _on_mqtt_message(client, userdata, msg):
    try:
        raw = json.loads(msg.payload.decode("utf-8", errors="ignore"))
    except Exception:
        return
    for sn in (SN_DELTA2, SN_EXTRA):
        if not sn:
            continue
        topics = _mqtt_topics(sn)
        with _mqtt_cache_lock:
            if msg.topic == topics["data"]:
                entry = _device_cache.setdefault(sn, {})
                entry.setdefault("quota", {}).update(raw.get("params", raw))
                entry["updated_at"] = time.time()
            elif msg.topic == topics["get_reply"] and raw.get("operateType") == "latestQuotas":
                d = raw.get("data", {})
                entry = _device_cache.setdefault(sn, {})
                entry["online"] = bool(d.get("online"))
                if d.get("online") and "quotaMap" in d:
                    entry.setdefault("quota", {}).update(d["quotaMap"])
                entry["updated_at"] = time.time()


def _on_mqtt_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        log.info("Conectado al MQTT privado de EcoFlow")
        for sn in (SN_DELTA2, SN_EXTRA):
            if not sn:
                continue
            topics = _mqtt_topics(sn)
            client.subscribe(topics["data"])
            client.subscribe(topics["get_reply"])
            _request_quota_refresh(sn)
    else:
        log.error("Fallo de conexión MQTT privado, rc=%s", rc)


def start_private_mqtt() -> None:
    """Login + credenciales MQTT + conexión persistente. Reintenta indefinidamente si falla."""
    global _mqtt_client, _mqtt_user_id
    if mqtt is None:
        log.error("Falta la librería paho-mqtt (agregala a requirements.txt)")
        return
    while True:
        try:
            token, user_id = _private_login()
            _mqtt_user_id = user_id
            url, port, username, password = _private_mqtt_creds(token)
            client_id = f"ANDROID_{uuid_lib.uuid4().hex.upper()}_{user_id}"
            client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)
            client.username_pw_set(username, password)
            client.tls_set()
            client.on_connect = _on_mqtt_connect
            client.on_message = _on_mqtt_message
            client.connect(url, port, keepalive=30)
            _mqtt_client = client
            log.info("Cliente MQTT privado iniciado (%s:%s)", url, port)
            client.loop_forever(retry_first_connection=True)
        except Exception:
            log.exception("Error en el cliente MQTT privado; reintento en 30s")
            time.sleep(30)


def get_device_quota(sn: str) -> dict:
    if USE_PRIVATE_API:
        _request_quota_refresh(sn)
        deadline = time.time() + 8
        while time.time() < deadline:
            with _mqtt_cache_lock:
                entry = _device_cache.get(sn)
                if entry and entry.get("quota") and time.time() - entry.get("updated_at", 0) < 6:
                    return entry["quota"]
            time.sleep(0.3)
        with _mqtt_cache_lock:
            entry = _device_cache.get(sn)
            if entry and entry.get("quota"):
                return entry["quota"]  # dato viejo, mejor que nada
        raise RuntimeError("Todavía no llegó ningún dato del dispositivo por MQTT")

    payload = _ecoflow_get("/iot-open/sign/device/quota/all", {"sn": sn})
    return payload.get("data", {})


def get_device_online(sn: str) -> bool:
    if USE_PRIVATE_API:
        with _mqtt_cache_lock:
            return bool(_device_cache.get(sn, {}).get("online"))

    payload = _ecoflow_get("/iot-open/sign/device/list", {})
    for dev in payload.get("data", []):
        if dev.get("sn") == sn:
            return dev.get("online") == 1
    return False


def _pick(data: dict, *keys, default=None):
    """Devuelve el primer valor presente entre varias claves posibles del quota map."""
    for key in keys:
        if key in data and data[key] is not None:
            return data[key]
    return default


def get_pv_watts(data: dict):
    # Confirmado con datos reales: mppt.inWatts ya viene directo en watts
    # (mppt.inWatts=501 ~= pd.wattsInSum=500 sin carga AC activa).
    return _pick(data, "mppt.inWatts")


def get_ac_watts(data: dict):
    """Potencia de entrada por corriente (cargador/pared), separada de la solar."""
    return _pick(data, "inv.inputWatts")


def format_delta2_report(data: dict, online: bool) -> str:
    soc = _pick(data, "bms_bmsStatus.soc", "pd.soc", "bmsMaster.soc")
    pv_w = get_pv_watts(data)
    ac_w = get_ac_watts(data)
    total_in_w = _pick(data, "pd.wattsInSum", default=(pv_w or 0) + (ac_w or 0))
    out_w = _pick(data, "pd.wattsOutSum", "inv.outputWatts", default=0)
    remain_min = _pick(data, "pd.remainTime", "bms_emsStatus.dsgRemainTime")
    temp = _pick(data, "bms_bmsStatus.temp", "inv.outTemp")

    lines = [f"🔋 *Delta 2* {'🟢 en línea' if online else '🔴 desconectada'}"]
    if soc is not None:
        lines.append(f"Carga: *{soc}%*")
    lines.append(f"☀️ Entrada solar (MPPT): *{pv_w if pv_w is not None else 'N/D'} W*")
    lines.append(f"🔌 Entrada por corriente (AC): *{ac_w if ac_w is not None else 'N/D'} W*")
    lines.append(f"⚡ Entrada total: {total_in_w} W · Salida: {out_w} W")
    if remain_min:
        hours, minutes = divmod(abs(int(remain_min)), 60)
        verb = "para llenarse" if int(remain_min) > 0 and total_in_w > out_w else "de autonomía"
        lines.append(f"⏱ ~{hours}h {minutes}m {verb}")
    if temp is not None:
        lines.append(f"🌡 Temperatura: {temp}°C")
    return "\n".join(lines)


def format_extra_battery_report(data: dict, online: bool) -> str:
    soc = _pick(data, "bms_bmsStatus.soc", "bmsSlave.soc", "kit.soc")
    temp = _pick(data, "bms_bmsStatus.temp", "bmsSlave.temp")
    lines = [f"🔋 *Batería extra* {'🟢 en línea' if online else '🔴 desconectada'}"]
    if soc is not None:
        lines.append(f"Carga: *{soc}%*")
    if temp is not None:
        lines.append(f"🌡 Temperatura: {temp}°C")
    return "\n".join(lines)


def send_telegram(text: str, chat_id: str = None) -> None:
    resp = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        json={"chat_id": chat_id or CHAT_ID, "text": text, "parse_mode": "Markdown"},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("ok"):
        raise RuntimeError(f"Telegram API error: {payload}")


def set_bot_commands() -> None:
    commands = [
        {"command": "start", "description": "Qué hace este bot"},
        {"command": "reporte", "description": "Informe detallado por dispositivo"},
        {"command": "estado", "description": "Línea rápida combinada"},
        {"command": "pausa", "description": "Pausar los informes automáticos"},
        {"command": "reanudar", "description": "Reanudar los informes automáticos"},
        {"command": "ping", "description": "Ver si el bot está activo"},
        {"command": "help", "description": "Ver comandos disponibles"},
    ]
    resp = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands", json={"commands": commands}, timeout=30
    )
    resp.raise_for_status()


# Delta 2 y batería extra son de la misma capacidad (1024Wh cada una), así que
# el promedio simple de sus %SOC es válido: ambas pesan lo mismo en el total.
BATTERY_CAPACITY_WH = 1024


def _combined_line(soc_delta2, soc_extra) -> str:
    if soc_delta2 is None or soc_extra is None:
        return ""
    avg = round((soc_delta2 + soc_extra) / 2, 1)
    total_wh = round(avg / 100 * BATTERY_CAPACITY_WH * 2)
    return f"🔷 *Total combinado*: *{avg}%* (~{total_wh} Wh de {BATTERY_CAPACITY_WH * 2} Wh)"


def build_report() -> str:
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    sections = [f"📊 *Informe EcoFlow* — {now}"]

    if not ECOFLOW_READY:
        sections.append(
            "⏳ EcoFlow todavía no está configurado (esperando ACCESS_KEY/SECRET_KEY "
            "de developer.ecoflow.com). Avisá cuando estén listas."
        )
        return "\n\n".join(sections)

    soc_delta2 = None
    soc_extra = None

    try:
        online = get_device_online(SN_DELTA2)
        data = get_device_quota(SN_DELTA2)
        soc_delta2 = _pick(data, "bms_bmsStatus.soc", "pd.soc", "bmsMaster.soc")
        sections.append(format_delta2_report(data, online))
    except Exception as exc:
        log.exception("Error consultando la Delta 2")
        sections.append(f"🔋 *Delta 2*\n⚠️ Error al consultar: {exc}")

    if SN_EXTRA:
        try:
            online = get_device_online(SN_EXTRA)
            data = get_device_quota(SN_EXTRA)
            soc_extra = _pick(data, "bms_bmsStatus.soc", "bmsSlave.soc", "kit.soc")
            sections.append(format_extra_battery_report(data, online))
        except Exception as exc:
            log.exception("Error consultando la batería extra")
            sections.append(f"🔋 *Batería extra*\n⚠️ Error al consultar: {exc}")

    combined = _combined_line(soc_delta2, soc_extra)
    if combined:
        sections.append(combined)

    return "\n\n".join(sections)


def build_quick_status() -> str:
    """Línea compacta y combinada: carga de ambas baterías + entrada solar/AC. Para /estado."""
    if not ECOFLOW_READY:
        return "⏳ EcoFlow todavía no está configurado."

    parts = []
    soc_delta2 = None
    soc_extra = None
    try:
        data = get_device_quota(SN_DELTA2)
        soc_delta2 = _pick(data, "bms_bmsStatus.soc", "pd.soc", "bmsMaster.soc")
        pv_w = get_pv_watts(data)
        ac_w = get_ac_watts(data)
        parts.append(f"Delta 2 {soc_delta2 if soc_delta2 is not None else 'N/D'}%")
        parts.append(f"☀️ {pv_w if pv_w is not None else 'N/D'} W")
        parts.append(f"🔌 {ac_w if ac_w is not None else 'N/D'} W")
    except Exception as exc:
        parts.append(f"Delta 2 ⚠️ {exc}")

    if SN_EXTRA:
        try:
            data = get_device_quota(SN_EXTRA)
            soc_extra = _pick(data, "bms_bmsStatus.soc", "bmsSlave.soc", "kit.soc")
            parts.append(f"Extra {soc_extra if soc_extra is not None else 'N/D'}%")
        except Exception as exc:
            parts.append(f"Extra ⚠️ {exc}")

    if soc_delta2 is not None and soc_extra is not None:
        parts.append(f"Total {round((soc_delta2 + soc_extra) / 2, 1)}%")

    return "🔋 " + " · ".join(parts)


HELP_TEXT = (
    "🤖 *Monitor EcoFlow*\n\n"
    "/reporte — informe detallado, por dispositivo (Delta 2 y batería extra)\n"
    "/estado — línea rápida combinada (carga de ambas + solar + AC)\n"
    "/pausa — pausar los informes automáticos\n"
    "/reanudar — reanudar los informes automáticos\n"
    "/ping — ver si el bot está activo\n"
    "/start — qué hace este bot\n"
    "/help — ver esta ayuda\n\n"
    f"Informe automático cada {INTERVAL_HOURS:g}h · chequeo de carga AC cada {AC_CHECK_MINUTES:g} min "
    "(si no está pausado)."
)
START_TEXT = (
    "👋 Hola, soy el monitor de tu EcoFlow.\n"
    f"Te mando un informe automático cada {INTERVAL_HOURS:g}h y te aviso apenas empiece a cargar "
    "por corriente.\n\n" + HELP_TEXT
)


def _format_uptime() -> str:
    seconds = int(time.time() - START_TIME)
    hours, rem = divmod(seconds, 3600)
    minutes = rem // 60
    return f"{hours}h {minutes}m"


def handle_command(text: str, chat_id: str) -> None:
    global AUTO_PAUSED
    cmd = text.strip().split()[0].split("@")[0].lower()
    if cmd == "/reporte":
        send_telegram(build_report(), chat_id=chat_id)
    elif cmd == "/estado":
        send_telegram(build_quick_status(), chat_id=chat_id)
    elif cmd == "/pausa":
        AUTO_PAUSED = True
        send_telegram("⏸ Informes automáticos pausados. Usá /reanudar para reactivarlos.", chat_id=chat_id)
    elif cmd == "/reanudar":
        AUTO_PAUSED = False
        send_telegram("▶️ Informes automáticos reanudados.", chat_id=chat_id)
    elif cmd == "/ping":
        estado_ecoflow = "🟢 EcoFlow configurado" if ECOFLOW_READY else "⏳ EcoFlow sin configurar"
        estado_auto = "⏸ pausado" if AUTO_PAUSED else "▶️ activo"
        send_telegram(
            f"🏓 Pong. Activo hace {_format_uptime()}.\n{estado_ecoflow} · Automático {estado_auto}",
            chat_id=chat_id,
        )
    elif cmd == "/start":
        send_telegram(START_TEXT, chat_id=chat_id)
    elif cmd == "/help":
        send_telegram(HELP_TEXT, chat_id=chat_id)


def poll_commands() -> None:
    """Escucha comandos entrantes (long polling) y responde. Ignora chats que no sean el configurado."""
    offset = 0
    while True:
        try:
            resp = requests.get(
                f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                params={"offset": offset, "timeout": 30},
                timeout=40,
            )
            resp.raise_for_status()
            payload = resp.json()
            for update in payload.get("result", []):
                offset = update["update_id"] + 1
                message = update.get("message", {})
                text = message.get("text", "")
                chat_id = str(message.get("chat", {}).get("id", ""))
                if not text.startswith("/"):
                    continue
                if chat_id != CHAT_ID:
                    log.info("Comando ignorado de chat no autorizado: %s", chat_id)
                    continue
                try:
                    handle_command(text, chat_id)
                except Exception:
                    log.exception("Error respondiendo comando %s", text)
        except Exception:
            log.exception("Error en el polling de comandos; reintento")
            time.sleep(5)


def report_timer() -> None:
    while True:
        time.sleep(INTERVAL_HOURS * 3600)
        if AUTO_PAUSED or not ECOFLOW_READY:
            log.info("Informe automático omitido (pausado o EcoFlow no configurado)")
            continue
        try:
            send_telegram(build_report())
            log.info("Informe periódico enviado")
        except Exception:
            log.exception("Fallo enviando el informe periódico")


def ac_check_timer() -> None:
    global WAS_CHARGING_AC
    while True:
        time.sleep(AC_CHECK_MINUTES * 60)
        if AUTO_PAUSED or not ECOFLOW_READY:
            continue
        try:
            data = get_device_quota(SN_DELTA2)
            ac_w = get_ac_watts(data)
            is_charging = bool(ac_w and ac_w > AC_WATTS_THRESHOLD)
            if is_charging and not WAS_CHARGING_AC:
                send_telegram(f"⚡ Llegó la corriente: la Delta 2 empezó a cargar por AC ({ac_w} W).")
                log.info("Notificado inicio de carga AC (%s W)", ac_w)
            WAS_CHARGING_AC = is_charging
        except Exception:
            log.exception("Error chequeando carga por AC")


def main() -> None:
    try:
        set_bot_commands()
    except Exception:
        log.exception("No se pudo registrar el menú de comandos (no bloqueante)")

    threading.Thread(target=poll_commands, daemon=True).start()
    threading.Thread(target=report_timer, daemon=True).start()
    threading.Thread(target=ac_check_timer, daemon=True).start()
    if USE_PRIVATE_API:
        threading.Thread(target=start_private_mqtt, daemon=True).start()

    log.info("Monitor iniciado. Informe cada %.1fh, chequeo AC cada %.1f min.", INTERVAL_HOURS, AC_CHECK_MINUTES)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
