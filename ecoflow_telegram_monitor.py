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

# Se persiste en un volumen de Railway (/data por default) para sobrevivir
# redeploys — sin esto, /pausa y "ya avisé que llegó la corriente" se
# resetean cada vez que se sube código nuevo.
STATE_FILE = os.environ.get("STATE_FILE", "/data/state.json")


def _load_persisted_state() -> dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _save_persisted_state() -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"auto_paused": AUTO_PAUSED, "was_charging_ac": WAS_CHARGING_AC}, f)
    except OSError:
        log.exception("No se pudo guardar el estado persistido en %s", STATE_FILE)


_persisted = _load_persisted_state()
AUTO_PAUSED = _persisted.get("auto_paused", False)
WAS_CHARGING_AC = _persisted.get("was_charging_ac", False)
_DATA_STALE_ALERTED = False

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
        # Los campos llegan en ráfaga, en mensajes separados por moduleType
        # (no una sola foto atómica). En vez de devolver apenas llega el
        # primer mensaje parcial, esperamos a que la ráfaga se calme (sin
        # mensajes nuevos por QUIET_S) para juntar más campos consistentes
        # entre sí, hasta un tope de MAX_WAIT_S.
        QUIET_S = 1.5
        MAX_WAIT_S = 6
        _request_quota_refresh(sn)
        start = time.time()
        while time.time() - start < MAX_WAIT_S:
            time.sleep(0.3)
            with _mqtt_cache_lock:
                entry = _device_cache.get(sn)
                if entry and entry.get("quota") and time.time() - entry.get("updated_at", 0) >= QUIET_S:
                    return entry["quota"]
        with _mqtt_cache_lock:
            entry = _device_cache.get(sn)
            if entry and entry.get("quota"):
                return entry["quota"]  # lo que haya juntado hasta el tope, mejor que nada
        raise RuntimeError("Todavía no llegó ningún dato del dispositivo por MQTT")

    payload = _ecoflow_get("/iot-open/sign/device/quota/all", {"sn": sn})
    return payload.get("data", {})


def get_device_online(sn: str) -> bool:
    if USE_PRIVATE_API:
        # El campo "online" solo llega en la respuesta a un pedido explícito
        # (get_reply), que a veces no alcanza a llegar antes de que
        # get_device_quota ya haya devuelto datos del canal de push. Si
        # recibimos datos frescos del dispositivo, ya sabemos que está online.
        with _mqtt_cache_lock:
            entry = _device_cache.get(sn, {})
            if entry.get("online"):
                return True
            return bool(entry.get("quota")) and (time.time() - entry.get("updated_at", 0) < 120)

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


AC_GAP_THRESHOLD_W = 500
NOISE_FLOOR_W = 10  # por debajo de esto es ruido de medición, no transferencia real


def classify_ac_and_battery_watts(data: dict, pv_w) -> tuple:
    """inv.inputWatts (dato real de corriente AC) suele venir ausente incluso
    cargando por AC. Como la transferencia hacia la batería extra ronda los
    30-65W (mucho menor a lo que carga un cargador de pared), un excedente
    grande entre el total y la solar (>500W) es mucho más probable que sea
    corriente real de la calle; un excedente chico es más probable que sea
    transferencia entre baterías. Devuelve (ac_w, battery_in_w)."""
    ac_w = get_ac_watts(data)
    if ac_w:
        return ac_w, 0
    total_in_w = _pick(data, "pd.wattsInSum", default=(pv_w or 0))
    gap = total_in_w - (pv_w or 0)
    if gap > AC_GAP_THRESHOLD_W:
        return round(gap), 0
    return 0, (max(0, round(gap)) if gap > NOISE_FLOOR_W else 0)


def get_extra_battery_soc(data: dict):
    """La batería extra no es un dispositivo aparte: sus datos (bms_slave.*)
    vienen incluidos en la misma respuesta de la Delta 2."""
    return _pick(data, "bms_slave.f32ShowSoc", "bms_slave.soc")


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
        {"command": "pausa", "description": "Pausar los informes automáticos"},
        {"command": "reanudar", "description": "Reanudar los informes automáticos"},
        {"command": "ping", "description": "Ver si el bot está activo"},
        {"command": "help", "description": "Ver comandos disponibles"},
    ]
    resp = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands", json={"commands": commands}, timeout=30
    )
    resp.raise_for_status()


# Delta 2 y batería extra son de la misma capacidad, así que el promedio
# simple de sus %SOC es válido: ambas pesan lo mismo en el total.
def _combined_line(soc_delta2, soc_extra) -> str:
    if soc_delta2 is None or soc_extra is None:
        return ""
    avg = round((soc_delta2 + soc_extra) / 2, 1)
    return f"🔋 *Total combinado*: *{avg:.1f}%*"


def build_report() -> str:
    if not ECOFLOW_READY:
        return (
            "📊 *Informe EcoFlow*\n\n"
            "⏳ EcoFlow todavía no está configurado (esperando ACCESS_KEY/SECRET_KEY "
            "de developer.ecoflow.com). Avisá cuando estén listas."
        )

    try:
        online = get_device_online(SN_DELTA2)
        data = get_device_quota(SN_DELTA2)
    except Exception as exc:
        log.exception("Error consultando la Delta 2")
        return f"📊 *Informe EcoFlow*\n\n⚠️ Error al consultar la Delta 2: {exc}"

    soc_delta2 = _pick(data, "bms_bmsStatus.f32ShowSoc", "bms_bmsStatus.soc", "pd.soc", "bmsMaster.soc")
    soc_extra = get_extra_battery_soc(data)
    extra_in_w = _pick(data, "bms_slave.inputWatts")
    pv_w = get_pv_watts(data)
    out_w = _pick(data, "pd.wattsOutSum", "inv.outputWatts", default=0)
    total_in_w = _pick(data, "pd.wattsInSum", default=(pv_w or 0))
    ac_w, battery_in_w = classify_ac_and_battery_watts(data, pv_w)
    remain_min = _pick(data, "pd.remainTime", "bms_emsStatus.dsgRemainTime")

    # En el encabezado, qué fuente está cargando ahora mismo (si alguna).
    # mppt.chgType == 2 confirmado empíricamente como "carga solar" en esta
    # Delta 2 (el mapeo documentado para River 2 no aplica igual acá). Todavía
    # no se pudo confirmar el valor real para corriente AC, así que ese caso
    # sigue con el umbral de excedente (>500W).
    chg_type = _pick(data, "mppt.chgType")
    is_solar_active = chg_type == 2 or (pv_w or 0) > AC_WATTS_THRESHOLD
    if (ac_w or 0) > AC_WATTS_THRESHOLD:
        source_emoji = " 🔌"
    elif is_solar_active:
        source_emoji = " ☀️"
    elif battery_in_w > 0 or out_w > 0:
        source_emoji = " 🔋"  # sin solar ni AC: corriendo (o transfiriendo) a batería
    else:
        source_emoji = ""
    status = "🟢 en línea" if online else "🔴 desconectada"

    lines = [f"📊 *Informe EcoFlow* · {status}{source_emoji}", ""]

    soc_delta2_str = f"{soc_delta2:.1f}" if soc_delta2 is not None else "N/D"
    lines.append(f"🔋 Delta 2 — Carga: *{soc_delta2_str}%*")
    if soc_extra is not None:
        lines.append(
            f"🔋 Batería Extra — Carga: *{soc_extra:.1f}%* "
            f"({extra_in_w if extra_in_w is not None else 0} W)"
        )
    combined = _combined_line(soc_delta2, soc_extra)
    if combined:
        lines.append(combined)
    lines.append("")

    lines.append(f"📥 Total entrada: {total_in_w} W")
    lines.append(f"☀️ Entrada solar: {pv_w if pv_w is not None else 'N/D'} W")
    lines.append(f"🔌 Entrada por corriente: {ac_w} W")
    lines.append(f"🪫 Entrada desde batería: {battery_in_w} W")
    lines.append(f"📤 Salida: {out_w} W")

    if remain_min:
        hours, minutes = divmod(abs(int(remain_min)), 60)
        verb = "para llenarse" if int(remain_min) > 0 and total_in_w > out_w else "de autonomía"
        lines.append(f"⏱ ~{hours}h {minutes}m {verb}")

    ports = [
        ("USB-C 1", _pick(data, "pd.typec1Watts")),
        ("USB-C 2", _pick(data, "pd.typec2Watts")),
        ("USB 1", _pick(data, "pd.usb1Watts")),
        ("USB 2", _pick(data, "pd.usb2Watts")),
        ("Auto (12V)", _pick(data, "pd.carWatts")),
    ]
    active_ports = [(name, w) for name, w in ports if w]
    if active_ports:
        lines.append("")
        lines.append("🔌 *Puertos activos:*")
        for name, w in active_ports:
            lines.append(f"  {name}: {w} W")

    return "\n".join(lines)


HELP_TEXT = (
    "🤖 *Monitor EcoFlow*\n\n"
    "/reporte — informe detallado, por dispositivo (Delta 2 y batería extra)\n"
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
    elif cmd == "/pausa":
        AUTO_PAUSED = True
        _save_persisted_state()
        send_telegram("⏸ Informes automáticos pausados. Usá /reanudar para reactivarlos.", chat_id=chat_id)
    elif cmd == "/reanudar":
        AUTO_PAUSED = False
        _save_persisted_state()
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
            pv_w = get_pv_watts(data)
            ac_w, _ = classify_ac_and_battery_watts(data, pv_w)
            is_charging = ac_w > AC_WATTS_THRESHOLD
            if is_charging and not WAS_CHARGING_AC:
                send_telegram(f"⚡ Llegó la corriente: la Delta 2 empezó a cargar por AC ({ac_w} W).")
                log.info("Notificado inicio de carga AC (%s W)", ac_w)
            if is_charging != WAS_CHARGING_AC:
                WAS_CHARGING_AC = is_charging
                _save_persisted_state()
        except Exception:
            log.exception("Error chequeando carga por AC")


WATCHDOG_CHECK_MINUTES = 5
WATCHDOG_STALE_MINUTES = 10  # sin datos frescos por más de esto = alerta


def watchdog_timer() -> None:
    """Avisa si dejamos de recibir datos del dispositivo por MQTT (conexión
    caída, credenciales vencidas, etc.) — sin esto, un corte silencioso solo
    se nota cuando el usuario nota que dejaron de llegar informes."""
    global _DATA_STALE_ALERTED
    while True:
        time.sleep(WATCHDOG_CHECK_MINUTES * 60)
        if AUTO_PAUSED or not ECOFLOW_READY or not USE_PRIVATE_API:
            continue
        with _mqtt_cache_lock:
            entry = _device_cache.get(SN_DELTA2, {})
            updated_at = entry.get("updated_at", 0)
        stale_for_min = (time.time() - updated_at) / 60 if updated_at else None
        is_stale = stale_for_min is None or stale_for_min > WATCHDOG_STALE_MINUTES
        try:
            if is_stale and not _DATA_STALE_ALERTED:
                minutos = f"{stale_for_min:.0f}" if stale_for_min is not None else "varios"
                send_telegram(
                    f"⚠️ No recibo datos de la Delta 2 hace {minutos} min. "
                    "Puede ser un corte de conexión MQTT o que las credenciales vencieron."
                )
                _DATA_STALE_ALERTED = True
                log.warning("Watchdog: datos viejos hace %s min, alertado", minutos)
            elif not is_stale and _DATA_STALE_ALERTED:
                send_telegram("✅ Volví a recibir datos de la Delta 2 con normalidad.")
                _DATA_STALE_ALERTED = False
                log.info("Watchdog: datos frescos de nuevo, alerta resuelta")
        except Exception:
            log.exception("Error en el watchdog de datos")


def main() -> None:
    try:
        set_bot_commands()
    except Exception:
        log.exception("No se pudo registrar el menú de comandos (no bloqueante)")

    threading.Thread(target=poll_commands, daemon=True).start()
    threading.Thread(target=report_timer, daemon=True).start()
    threading.Thread(target=ac_check_timer, daemon=True).start()
    threading.Thread(target=watchdog_timer, daemon=True).start()
    if USE_PRIVATE_API:
        threading.Thread(target=start_private_mqtt, daemon=True).start()

    log.info("Monitor iniciado. Informe cada %.1fh, chequeo AC cada %.1f min.", INTERVAL_HOURS, AC_CHECK_MINUTES)
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
