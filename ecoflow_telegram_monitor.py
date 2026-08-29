#!/usr/bin/env python3
"""
Monitor EcoFlow (Delta 2 + batería extra) → informe por Telegram.

Un solo proceso, corre 24/7 en Railway: escucha comandos de Telegram al
instante (long polling) y en paralelo chequea cada cierto tiempo si toca
mandar el informe periódico o si empezó a cargar por corriente. Todo el
estado (última carga AC, umbrales de alerta) se persiste en /data/state.json.

Variables de entorno requeridas:
  ECOFLOW_ACCESS_KEY   Access key de developer.ecoflow.com
  ECOFLOW_SECRET_KEY   Secret key de developer.ecoflow.com
  ECOFLOW_SN_DELTA2    Número de serie de la Delta 2
  ECOFLOW_SN_EXTRA     Número de serie de la batería extra (opcional)
  TELEGRAM_BOT_TOKEN   Token del bot (de @BotFather)
  TELEGRAM_CHAT_ID     Chat ID destino (de @userinfobot)
  AC_CHECK_MINUTES     Cada cuánto chequear si empezó a cargar por AC (default: 1)

Uso:
  python3 ecoflow_telegram_monitor.py
"""

import base64
import hashlib
import hmac
import http.server
import json
import logging
import os
import random
import sys
import threading
import time
import unicodedata
import uuid as uuid_lib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

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
AC_CHECK_MINUTES = float(os.environ.get("AC_CHECK_MINUTES", "1"))
AC_WATTS_THRESHOLD = 5  # por debajo de esto se considera "no está cargando por AC"

# Railway corre en UTC; esto es solo para mostrar horas locales (hora estimada
# de autonomía, bloques del plan de cargas). zoneinfo maneja el horario de
# verano de Cuba automáticamente.
TZ = ZoneInfo(os.environ.get("TZ_NAME", "America/Havana"))
# Horario silencioso: de noche no tiene sentido recibir el informe automático
# cada media hora. Las alertas (llegó/se fue la luz, batería baja/llena) siguen
# funcionando igual, solo se pausa el informe periódico.
QUIET_START_HOUR = int(os.environ.get("QUIET_START_HOUR", "23"))
QUIET_START_MINUTE = int(os.environ.get("QUIET_START_MINUTE", "30"))
QUIET_END_HOUR = int(os.environ.get("QUIET_END_HOUR", "7"))
QUIET_END_MINUTE = int(os.environ.get("QUIET_END_MINUTE", "0"))

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

# Se persiste en un volumen de Railway (/data por default) para sobrevivir
# redeploys — sin esto, "ya avisé que llegó la corriente" y los umbrales de
# alerta se resetean cada vez que se sube código nuevo.
STATE_FILE = os.environ.get("STATE_FILE", "/data/state.json")


def _load_persisted_state() -> dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


# Mapa de cargas que el usuario puede marcar ON/OFF a mano (por Telegram o el
# panel web) para que el bot sepa qué está realmente encendido en vez de
# asumirlo solo por el horario del plan. Los watts son los reales confirmados
# por el usuario durante la sesión, se reusan para el chequeo de anomalía en
# build_load_advisor_message(). Las cargas con más de una unidad (ventilador,
# power bank) se desglosan una por una en vez de un solo interruptor
# combinado, para poder marcar exactamente cuáles están prendidas. Las luces
# se sacaron de este mapa: se prenden/apagan tan seguido que registrarlas a
# mano con /on-/off era más carga que valor.
MULTI_UNIT_DEVICES = {"ventilador": (3, "Ventilador", "🌀", 20), "powerbank": (2, "Power bank", "🔋", 60)}

DEVICE_INFO = {
    "nevera": {"label": "Nevera", "emoji": "🥶", "watts": 100},
    "laptop": {"label": "Laptop", "emoji": "💻", "watts": 160},
    "tv": {"label": "TV", "emoji": "📺", "watts": 100},
}
for _base, (_count, _label, _emoji, _watts) in MULTI_UNIT_DEVICES.items():
    for _i in range(1, _count + 1):
        DEVICE_INFO[f"{_base}{_i}"] = {"label": f"{_label} {_i}", "emoji": _emoji, "watts": _watts}
DEVICE_INFO["ventilador3"]["watts"] = 10  # el tercer ventilador es más chico que los otros dos (20W)

INTERNET_WATTS = 45  # promedio del rango 30-60 W, siempre ON, no se marca a mano


def _save_persisted_state() -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "was_charging_ac": WAS_CHARGING_AC,
                    "battery_low_threshold": BATTERY_LOW_THRESHOLD,
                    "was_below_low_threshold": WAS_BELOW_LOW_THRESHOLD,
                    "was_full": WAS_FULL,
                    "last_ac_timestamp": LAST_AC_TIMESTAMP,
                    "device_state": DEVICE_STATE,
                },
                f,
            )
    except OSError:
        log.exception("No se pudo guardar el estado persistido en %s", STATE_FILE)


_persisted = _load_persisted_state()
WAS_CHARGING_AC = _persisted.get("was_charging_ac", False)
BATTERY_LOW_THRESHOLD = _persisted.get("battery_low_threshold", 20)
WAS_BELOW_LOW_THRESHOLD = _persisted.get("was_below_low_threshold", False)
WAS_FULL = _persisted.get("was_full", False)
LAST_AC_TIMESTAMP = _persisted.get("last_ac_timestamp")
_saved_device_state = _persisted.get("device_state", {})
DEVICE_STATE = {key: bool(_saved_device_state.get(key, False)) for key in DEVICE_INFO}
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
    # Sin Content-Type: application/json a propósito. La doc oficial dice que
    # con ese header el servidor busca los datos a firmar en el body; nuestros
    # GET mandan los datos por query string, así que con ese header puesto la
    # firma no coincide del lado del servidor (confirmado: rompía quota/all,
    # que sí lleva parámetros, pero no rompía device/list, que no lleva
    # ninguno — por eso pasaba desapercibido).
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


def get_device_quota_cached(sn: str) -> dict:
    """Lee el último dato que ya llegó por el canal push de MQTT (el
    dispositivo empuja telemetría solo, sin que se le pida), sin pedir un
    refresh activo ni esperar. Para el dashboard, que puede consultar cada
    1-5s sin golpear la conexión MQTT con un pedido nuevo cada vez."""
    if not USE_PRIVATE_API:
        return get_device_quota(sn)
    with _mqtt_cache_lock:
        entry = _device_cache.get(sn)
        if entry and entry.get("quota"):
            return entry["quota"]
    raise RuntimeError("Todavía no llegó ningún dato del dispositivo por MQTT")


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
    """Potencia de entrada por corriente (cargador/pared). Campo directo
    confirmado por la doc oficial de EcoFlow (inv.inputWatts = "Charging
    power (W)")."""
    return _pick(data, "inv.inputWatts")


NOISE_FLOOR_W = 5  # por debajo de esto es ruido de medición, no transferencia real
AC_VOLTAGE_THRESHOLD_MV = 50000  # ~50V; AC real ronda 110000-240000 mV, esto solo filtra ausencia/ruido


def get_ac_present(data: dict) -> bool:
    """AC físicamente conectado, sin importar si está cargando, en modo
    paso-directo, o si la batería ya está llena. A diferencia del wattage
    neto (que cae a ~0 en paso-directo y daba falsos "se fue la luz"), esto
    mide el voltaje de entrada del inversor directamente. Campo confirmado
    en la doc oficial de EcoFlow (inv.acInVol)."""
    ac_in_vol = _pick(data, "inv.acInVol")
    return bool(ac_in_vol and ac_in_vol > AC_VOLTAGE_THRESHOLD_MV)


def classify_ac_and_battery_watts(data: dict, pv_w) -> tuple:
    """Devuelve (ac_w, battery_in_w). Si hay AC conectado (confirmado por
    voltaje, no por wattage), ac_w es inv.inputWatts —puede ser 0 en
    paso-directo y sigue siendo AC real—. Si no hay AC, cualquier excedente
    sobre la solar se interpreta como transferencia entre baterías."""
    if get_ac_present(data):
        return (get_ac_watts(data) or 0), 0
    total_in_w = _pick(data, "pd.wattsInSum", default=(pv_w or 0))
    gap = total_in_w - (pv_w or 0)
    return 0, (max(0, round(gap)) if gap > NOISE_FLOOR_W else 0)


def get_extra_battery_soc(data: dict):
    """La batería extra no es un dispositivo aparte: sus datos (bms_slave.*)
    vienen incluidos en la misma respuesta de la Delta 2."""
    return _pick(data, "bms_slave.f32ShowSoc", "bms_slave.soc")


_sent_message_ids = []
_sent_message_lock = threading.Lock()


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
    msg_id = payload.get("result", {}).get("message_id")
    if msg_id and (chat_id or CHAT_ID) == CHAT_ID:
        with _sent_message_lock:
            _sent_message_ids.append(msg_id)


def set_bot_commands() -> None:
    commands = [
        {"command": "start", "description": "Qué hace este bot"},
        {"command": "reporte", "description": "Informe detallado por dispositivo"},
        {"command": "cargas", "description": "Qué encender/apagar ahora según el plan"},
        {"command": "on", "description": "Marcar un dispositivo como encendido (ej: /on laptop)"},
        {"command": "off", "description": "Marcar un dispositivo como apagado (ej: /off tv)"},
        {"command": "alerta", "description": "Avisar cuando la carga baje de X% (ej: /alerta 20)"},
        {"command": "help", "description": "Ver comandos disponibles"},
    ]
    resp = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands", json={"commands": commands}, timeout=30
    )
    resp.raise_for_status()


DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "").strip()


def set_dashboard_menu_button() -> None:
    """Pone el botón del menú (al lado del clip, abajo a la izquierda) para
    que abra el dashboard como Web App adentro de Telegram, sin salir a un
    navegador aparte."""
    if not DASHBOARD_URL:
        return
    resp = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/setChatMenuButton",
        json={"menu_button": {"type": "web_app", "text": "📊 Panel", "web_app": {"url": DASHBOARD_URL}}},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("ok"):
        log.warning("No se pudo configurar el botón del dashboard: %s", payload)


def _charge_source(pv_w, ac_present, ac_w, system_net_w) -> tuple:
    """(verbo, emoji) de qué está alimentando al sistema (Delta 2 + batería
    extra) en este momento: corriente de la calle > solar (solo o + batería
    si la solar no alcanza) > solo batería. Con AC conectado pero en
    paso-directo (batería llena, ac_w~0) dice "Usando" en vez de "Cargando
    por", porque no está entrando energía neta a la batería aunque el cable
    siga puesto. Usa el neto de TODO el sistema, no solo el de la Delta 2,
    porque la batería que ayuda puede ser la extra."""
    has_solar = bool(pv_w and pv_w > NOISE_FLOOR_W)
    if ac_present:
        verb = "Cargando por" if ac_w and ac_w > NOISE_FLOOR_W else "Usando"
        return verb, "🔌"
    battery_helping = system_net_w is not None and system_net_w < -NOISE_FLOOR_W
    if has_solar and battery_helping:
        return "Usando", "☀️/🔋"
    if has_solar:
        return "Cargando por", "☀️"
    return "Usando", "🔋"


def _battery_flow_emoji(net_w) -> tuple:
    """(emoji_batería, etiqueta, sufijo_texto) según el neto: 🔋 + Carga + 🔌 si
    carga, 🪫 + Descarga si descarga, 🔋 + Carga si no hay flujo."""
    if net_w is None or -NOISE_FLOOR_W <= net_w <= NOISE_FLOOR_W:
        return "🔋", "Carga", ""
    if net_w > NOISE_FLOOR_W:
        return "🔋", "Carga", f" ({round(net_w)} W)"
    return "🪫", "Descarga", f" ({abs(round(net_w))} W)"


# Delta 2 y batería extra son de la misma capacidad, así que el promedio
# simple de sus %SOC es válido: ambas pesan lo mismo en el total.
def _combined_line(soc_delta2, soc_extra, system_net_w) -> str:
    if soc_delta2 is None or soc_extra is None:
        return ""
    avg = round((soc_delta2 + soc_extra) / 2, 1)
    emoji = "🪫" if system_net_w is not None and system_net_w < -NOISE_FLOOR_W else "🔋"
    return f"{emoji} *Total del sistema*: *{avg:.1f}%*"


BATTERY_CAPACITY_WH = 1024  # Delta 2 y la batería extra son 1024Wh cada una


def _time_to_threshold_line(soc, net_w, num_batteries, threshold) -> str:
    """Estimación lineal (mismo criterio que pd.remainTime del propio
    dispositivo) de cuánto falta para que la carga llegue al umbral de
    batería baja configurado con /alerta."""
    if soc is None or net_w is None or net_w >= -NOISE_FLOOR_W or soc <= threshold:
        return ""
    capacity_wh = BATTERY_CAPACITY_WH * num_batteries
    energy_to_burn_wh = capacity_wh * (soc - threshold) / 100
    hours = energy_to_burn_wh / (-net_w)
    eta = datetime.now(TZ) + timedelta(hours=hours)
    h, m = divmod(int(round(hours * 60)), 60)
    return f"🪫 ~{h}h {m}m para llegar al {threshold}% (a las {eta.strftime('%H:%M')})"


def _format_elapsed(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hours, rem = divmod(seconds, 3600)
    minutes = rem // 60
    if hours >= 24:
        days, hours = divmod(hours, 24)
        return f"{days}d {hours}h"
    return f"{hours}h {minutes}m"


def _last_ac_line() -> str:
    if not LAST_AC_TIMESTAMP:
        return "⚡ Última vez que llegó corriente: sin registro"
    return f"⚡ Última vez que llegó corriente: hace {_format_elapsed(time.time() - LAST_AC_TIMESTAMP)}"


def _gather_metrics(passive: bool = False) -> dict:
    """Junta y deriva todos los datos de un vistazo: la usan tanto el informe
    de Telegram como el dashboard web, para no duplicar la lógica de campos
    confiables/no confiables que costó tanto afinar. passive=True lee el
    caché sin pedir un refresh activo (para el dashboard, que consulta muy
    seguido)."""
    data = get_device_quota_cached(SN_DELTA2) if passive else get_device_quota(SN_DELTA2)

    soc_delta2 = _pick(data, "bms_bmsStatus.f32ShowSoc", "bms_bmsStatus.soc", "pd.soc", "bmsMaster.soc")
    soc_extra = get_extra_battery_soc(data)
    extra_in_w = _pick(data, "bms_slave.inputWatts")
    extra_out_w = _pick(data, "bms_slave.outputWatts")
    pv_w = get_pv_watts(data)
    out_w = _pick(data, "pd.wattsOutSum", "inv.outputWatts", default=0)
    total_in_w = _pick(data, "pd.wattsInSum", default=(pv_w or 0))
    remain_min = _pick(data, "pd.remainTime", "bms_emsStatus.dsgRemainTime")

    # bms_bmsStatus.inputWatts/outputWatts (campo directo de la Delta 2) está
    # confirmado roto (pegado en 0); se deriva el neto de la contabilidad
    # general del sistema en su lugar. Para la batería extra sí hay un campo
    # de entrada confiable (bms_slave.inputWatts).
    delta2_net_w = total_in_w - out_w
    extra_net_w = (extra_in_w or 0) - (extra_out_w or 0) if extra_in_w is not None else None
    system_net_w = delta2_net_w + (extra_net_w or 0)
    ac_present = get_ac_present(data)
    ac_w, _ = classify_ac_and_battery_watts(data, pv_w)
    source_verb, source_emoji = _charge_source(pv_w, ac_present, ac_w, system_net_w)

    # Si falla la lectura de una de las dos (hueco momentáneo de datos por
    # MQTT), promediar solo con la que sí llegó en vez de perder avg_soc del
    # todo — importa para que el chequeo de emergencia de batería no se quede
    # ciego justo cuando más hace falta.
    if soc_delta2 is not None and soc_extra is not None:
        avg_soc = round((soc_delta2 + soc_extra) / 2, 1)
    elif soc_delta2 is not None:
        avg_soc = soc_delta2
    elif soc_extra is not None:
        avg_soc = soc_extra
    else:
        avg_soc = None

    remain = None
    is_stable = abs(total_in_w - out_w) <= NOISE_FLOOR_W
    if is_stable:
        remain = {"stable": True}
    elif remain_min:
        hours, minutes = divmod(abs(int(remain_min)), 60)
        # La doc oficial dice que el signo de pd.remainTime basta para saber
        # la dirección, pero en este equipo se confirmó que el campo se queda
        # pegado en un valor viejo mientras el wattage real sigue cambiando
        # (mismo patrón que bms_bmsStatus.inputWatts, que también está roto
        # acá). Volvemos a comparar entrada vs salida, que sí es confiable.
        charging_up = total_in_w > out_w
        eta = datetime.now(TZ) + timedelta(minutes=abs(int(remain_min)))
        remain = {
            "stable": False,
            "hours": hours,
            "minutes": minutes,
            "charging_up": charging_up,
            "eta": eta.strftime("%H:%M"),
        }

    if soc_extra is not None:
        threshold_line = _time_to_threshold_line(avg_soc, system_net_w, 2, BATTERY_LOW_THRESHOLD)
    else:
        threshold_line = _time_to_threshold_line(soc_delta2, delta2_net_w, 1, BATTERY_LOW_THRESHOLD)

    ports = [
        ("USB-C", _pick(data, "pd.typec1Watts")),
        ("USB-C", _pick(data, "pd.typec2Watts")),
        ("USB-A", _pick(data, "pd.usb1Watts")),
        ("USB-A", _pick(data, "pd.usb2Watts")),
        ("Auto (12V)", _pick(data, "pd.carWatts")),
    ]
    active_ports = [{"name": name, "watts": w} for name, w in ports if w]

    return {
        "soc_delta2": soc_delta2,
        "soc_extra": soc_extra,
        "avg_soc": avg_soc,
        "delta2_net_w": delta2_net_w,
        "extra_net_w": extra_net_w,
        "system_net_w": system_net_w,
        "pv_w": pv_w,
        "ac_w": ac_w,
        "has_ac": ac_present,
        "out_w": out_w,
        "total_in_w": total_in_w,
        "source_verb": source_verb,
        "source_emoji": source_emoji,
        "remain": remain,
        "threshold_line": threshold_line,
        "ports": active_ports,
    }


def _format_report(m: dict) -> str:
    lines = [f"📊 *Informe EcoFlow* · {m['source_verb']} {m['source_emoji']}", ""]

    # 1. Datos del sistema (lo más importante: cuánta carga queda)
    lines.append("📋 *Datos del sistema*")
    delta2_emoji, delta2_label, delta2_suffix = _battery_flow_emoji(m["delta2_net_w"])
    soc_delta2_str = f"{m['soc_delta2']:.1f}" if m["soc_delta2"] is not None else "N/D"
    lines.append(f"{delta2_emoji} Delta 2 — {delta2_label}: *{soc_delta2_str}%*{delta2_suffix}")
    if m["soc_extra"] is not None:
        extra_emoji, extra_label, extra_suffix = _battery_flow_emoji(m["extra_net_w"])
        lines.append(f"{extra_emoji} Batería Extra — {extra_label}: *{m['soc_extra']:.1f}%*{extra_suffix}")
    combined = _combined_line(m["soc_delta2"], m["soc_extra"], m["system_net_w"])
    if combined:
        lines.append(combined)
    lines.append("")

    # 2. Flujo de energía
    lines.append("🔄 *Flujo de energía*")
    lines.append(f"📥 Entrada total: {m['total_in_w']} W")
    lines.append(f"☀️ Entrada solar: {m['pv_w'] if m['pv_w'] is not None else 'N/D'} W")
    lines.append(f"📤 Salida: {m['out_w']} W")
    if m["remain"] and m["remain"]["stable"]:
        lines.append("⚖️ Estable: entra casi lo mismo que sale, ni carga ni descarga neta")
    elif m["remain"]:
        r = m["remain"]
        verb = "para llenarse" if r["charging_up"] else "de autonomía"
        eta_verb = "lleno a las" if r["charging_up"] else "dura hasta las"
        lines.append(f"⏱ ~{r['hours']}h {r['minutes']}m {verb} ({eta_verb} {r['eta']})")
    if m["threshold_line"]:
        lines.append(m["threshold_line"])
    lines.append("")

    # 3. Puertos (solo si hay algo conectado)
    if m["ports"]:
        lines.append("🔗 *Puertos*")
        for p in m["ports"]:
            lines.append(f"  {p['name']}: {p['watts']} W")
        lines.append("")

    # 4. Corriente
    lines.append(f"🔌 ¿Hay corriente?: {'Sí (' + str(m['ac_w']) + ' W)' if m['has_ac'] else 'No'}")
    lines.append(_last_ac_line())

    return "\n".join(lines)


def build_report(m: dict = None) -> str:
    if not ECOFLOW_READY:
        return (
            "📊 *Informe EcoFlow*\n\n"
            "⏳ EcoFlow todavía no está configurado (esperando ACCESS_KEY/SECRET_KEY "
            "de developer.ecoflow.com). Avisá cuando estén listas."
        )
    if m is None:
        try:
            m = _gather_metrics()
        except Exception as exc:
            log.exception("Error consultando la Delta 2")
            return f"📊 *Informe EcoFlow*\n\n⚠️ Error al consultar la Delta 2: {exc}"
    return _format_report(m)



HELP_TEXT = (
    "🤖 *Monitor EcoFlow*\n\n"
    "/reporte — informe detallado, por dispositivo (Delta 2 y batería extra)\n"
    "/cargas — qué debería estar encendido/apagado ahora mismo según el plan\n"
    "/on <dispositivo> — marcarlo encendido (nevera, laptop, tv, ventilador, powerbank)\n"
    "/off <dispositivo> — marcarlo apagado\n"
    "/alerta <porcentaje> — avisar cuando la carga baje de ese nivel (ej: /alerta 20)\n"
    "/start — qué hace este bot\n"
    "/help — ver esta ayuda\n\n"
    f"Informe automático a las :00 y :30 de cada hora (pausado de {QUIET_START_HOUR:02d}:{QUIET_START_MINUTE:02d} a "
    f"{QUIET_END_HOUR:02d}:{QUIET_END_MINUTE:02d}) — incluye el detalle de batería/puertos y, en el mismo mensaje, "
    "qué encender/apagar según el plan. Chequeo de carga AC cada "
    f"{AC_CHECK_MINUTES:g} min, también te aviso al llegar a 100% de carga.\n\n"
    "⚠️ Y si el ritmo de descarga proyecta que vas a llegar corto a la meta "
    "(65-75% a las 3 PM, 100% al anochecer si te mantenés en nevera+internet), "
    "te aviso antes de que pase."
)
START_TEXT = "👋 Hola, soy el monitor de tu EcoFlow.\n\n" + HELP_TEXT


_DEVICE_ALIASES = {
    "tele": "tv", "television": "tv", "televisor": "tv",
    "power": "powerbank", "bank": "powerbank", "powerbanks": "powerbank",
    "ventiladores": "ventilador", "fan": "ventilador", "fans": "ventilador",
    "pc": "laptop", "compu": "laptop", "computadora": "laptop",
}


def _resolve_device_keys(word: str) -> list:
    """Saca acentos/espacios y matchea contra DEVICE_INFO (o un alias común).
    Para las cargas con varias unidades (ventilador, powerbank):
    con número al final devuelve esa unidad puntual (ej. 'ventilador2' o
    'fan 2' → ['ventilador2']); sin número devuelve TODAS las unidades de esa
    categoría (ej. 'powerbank' → ['powerbank1','powerbank2']). Lista vacía si
    no reconoce nada."""
    word = word.strip().lower().replace(" ", "")
    word = "".join(c for c in unicodedata.normalize("NFD", word) if unicodedata.category(c) != "Mn")
    match = None
    for i in range(len(word), 0, -1):
        if word[:i].isalpha():
            match = (word[:i], word[i:])
            break
    if match is None:
        return []
    base, num = match
    base = _DEVICE_ALIASES.get(base, base)
    if base in MULTI_UNIT_DEVICES:
        count = MULTI_UNIT_DEVICES[base][0]
        if num:
            key = f"{base}{num}"
            return [key] if key in DEVICE_INFO else []
        return [f"{base}{i}" for i in range(1, count + 1)]
    if num:
        return []  # "tv2" no existe, es de una sola unidad
    return [base] if base in DEVICE_INFO else []


def handle_command(text: str, chat_id: str) -> None:
    global BATTERY_LOW_THRESHOLD
    parts = text.strip().split()
    cmd = parts[0].split("@")[0].lower()
    if cmd == "/reporte":
        send_telegram(build_report(), chat_id=chat_id)
    elif cmd == "/cargas":
        msg = build_load_advisor_message()
        if not msg:
            msg = (
                "🌙 Fuera de franja (6:00 AM–12:00 AM): internet ON, resto OFF, nevera "
                "apagada desde las 12 AM hasta el amanecer."
            )
        send_telegram(msg, chat_id=chat_id)
    elif cmd in ("/on", "/off"):
        if len(parts) < 2:
            send_telegram(f"Uso: {cmd} <dispositivo> — {', '.join(DEVICE_INFO)}", chat_id=chat_id)
        else:
            keys = _resolve_device_keys(parts[1])
            if not keys:
                send_telegram(f"No reconozco '{parts[1]}'. Dispositivos: {', '.join(DEVICE_INFO)}", chat_id=chat_id)
            else:
                estado = "ON" if cmd == "/on" else "OFF"
                for key in keys:
                    DEVICE_STATE[key] = cmd == "/on"
                _save_persisted_state()
                lines = [f"{DEVICE_INFO[k]['emoji']} {DEVICE_INFO[k]['label']}: marcado {estado}" for k in keys]
                send_telegram("\n".join(lines), chat_id=chat_id)
    elif cmd == "/alerta":
        if len(parts) < 2 or not parts[1].isdigit() or not (0 <= int(parts[1]) <= 100):
            send_telegram("Uso: /alerta <porcentaje entre 0 y 100>, ej: /alerta 20", chat_id=chat_id)
        else:
            BATTERY_LOW_THRESHOLD = int(parts[1])
            _save_persisted_state()
            send_telegram(f"🔔 Te voy a avisar cuando la carga baje de {BATTERY_LOW_THRESHOLD}%.", chat_id=chat_id)
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


def _seconds_until_next_slot() -> float:
    """Próximo :00 o :30 en punto (hora local), para que el informe automático
    llegue siempre en esos horarios en vez de a minutos sueltos según cuándo
    arrancó el contenedor."""
    now = datetime.now(TZ)
    if now.minute < 30:
        next_slot = now.replace(minute=30, second=0, microsecond=0)
    else:
        next_slot = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    return (next_slot - now).total_seconds()


_QUIET_START_MIN = QUIET_START_HOUR * 60 + QUIET_START_MINUTE
_QUIET_END_MIN = QUIET_END_HOUR * 60 + QUIET_END_MINUTE


def _in_quiet_hours(now=None) -> bool:
    now = now or datetime.now(TZ)
    minute_of_day = now.hour * 60 + now.minute
    if _QUIET_START_MIN > _QUIET_END_MIN:  # el rango cruza la medianoche
        return minute_of_day >= _QUIET_START_MIN or minute_of_day < _QUIET_END_MIN
    return _QUIET_START_MIN <= minute_of_day < _QUIET_END_MIN


def build_combined_message() -> str:
    """Junta el informe detallado y la gestión de cargas en un solo mensaje
    de Telegram — antes eran dos timers separados que mandaban dos mensajes
    casi al mismo minuto, con contenido parecido (batería, entrada/salida)
    aunque no idéntico. Un solo envío, más fácil de leer de una. Lee
    _gather_metrics() UNA sola vez y se la pasa a ambas partes — antes cada
    una hacía su propia lectura por separado, el doble de consultas a la
    Delta 2 para un mismo mensaje y con riesgo de números levemente
    distintos entre secciones si el dato cambiaba en el medio."""
    if not ECOFLOW_READY:
        return build_report()
    try:
        m = _gather_metrics()
    except Exception as exc:
        log.exception("Error consultando la Delta 2")
        return f"📊 *Informe EcoFlow*\n\n⚠️ Error al consultar la Delta 2: {exc}"
    report = build_report(m)
    cargas = build_load_advisor_message(m)
    if not cargas:
        return report
    return report + "\n\n➖➖➖➖➖➖➖➖➖➖\n\n" + cargas


def report_timer() -> None:
    while True:
        time.sleep(_seconds_until_next_slot())
        if not ECOFLOW_READY:
            log.info("Informe automático omitido (EcoFlow no configurado)")
            continue
        if _in_quiet_hours():
            log.info("Informe automático omitido (horario silencioso)")
            continue
        try:
            send_telegram(build_combined_message())
            log.info("Informe periódico (con gestión de cargas) enviado")
        except Exception:
            log.exception("Fallo enviando el informe periódico")


_quiet_mode_active = False


def quiet_hours_timer() -> None:
    """Avisa al entrar y salir del horario silencioso, para que quede claro
    que el informe automático se pausó a propósito y no por una falla."""
    global _quiet_mode_active
    while True:
        time.sleep(300)
        if not ECOFLOW_READY:
            continue
        now_quiet = _in_quiet_hours()
        try:
            if now_quiet and not _quiet_mode_active:
                send_telegram(
                    f"🌙 Entrando en horario silencioso ({QUIET_START_HOUR:02d}:{QUIET_START_MINUTE:02d}–"
                    f"{QUIET_END_HOUR:02d}:{QUIET_END_MINUTE:02d}): pauso los informes automáticos hasta las "
                    f"{QUIET_END_HOUR:02d}:{QUIET_END_MINUTE:02d}. Las alertas siguen activas."
                )
                _quiet_mode_active = True
                log.info("Horario silencioso activado")
            elif not now_quiet and _quiet_mode_active:
                send_telegram("☀️ Salgo del horario silencioso, retoman los informes automáticos.")
                _quiet_mode_active = False
                log.info("Horario silencioso desactivado")
        except Exception:
            log.exception("Error en el aviso de horario silencioso")


WEEKLY_CLEANUP_WEEKDAY = int(os.environ.get("WEEKLY_CLEANUP_WEEKDAY", "6"))  # 0=lunes … 6=domingo
WEEKLY_CLEANUP_HOUR = int(os.environ.get("WEEKLY_CLEANUP_HOUR", "4"))
_last_cleanup_week = None


def _delete_telegram_message(message_id: int) -> None:
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage",
            json={"chat_id": CHAT_ID, "message_id": message_id},
            timeout=15,
        )
    except Exception:
        log.exception("No se pudo borrar el mensaje %s", message_id)


def weekly_cleanup_timer() -> None:
    """Borra una vez por semana los mensajes que mandó el bot (solo los
    propios: Telegram no deja que un bot borre mensajes de chats privados que
    mandó el usuario)."""
    global _last_cleanup_week
    while True:
        time.sleep(600)
        now = datetime.now(TZ)
        week_key = now.isocalendar()[:2]
        if now.weekday() != WEEKLY_CLEANUP_WEEKDAY or now.hour != WEEKLY_CLEANUP_HOUR or _last_cleanup_week == week_key:
            continue
        with _sent_message_lock:
            ids = list(_sent_message_ids)
            _sent_message_ids.clear()
        for msg_id in ids:
            _delete_telegram_message(msg_id)
        _last_cleanup_week = week_key
        log.info("Limpieza semanal: %d mensajes borrados", len(ids))
        try:
            send_telegram("🧹 Limpieza semanal del chat hecha.")
        except Exception:
            log.exception("Error avisando la limpieza semanal")


FULL_CHARGE_THRESHOLD = 99  # % a partir del cual se considera "carga completa"
FULL_CHARGE_RESET_THRESHOLD = 95  # baja de esto para poder volver a avisar


def ac_check_timer() -> None:
    """Chequea, en un mismo ciclo: si llegó la corriente, si la carga bajó del
    umbral configurado con /alerta, y si terminó de cargar (100%)."""
    global WAS_CHARGING_AC, WAS_BELOW_LOW_THRESHOLD, WAS_FULL, LAST_AC_TIMESTAMP
    while True:
        time.sleep(AC_CHECK_MINUTES * 60)
        if not ECOFLOW_READY:
            continue
        try:
            data = get_device_quota(SN_DELTA2)
            pv_w = get_pv_watts(data)
            ac_w, _ = classify_ac_and_battery_watts(data, pv_w)
            # Presencia real de AC (por voltaje), no por wattage neto — así no
            # se dispara "se fue la luz" cuando la batería está llena y el AC
            # sigue enchufado en paso-directo (0W netos pero AC presente).
            is_charging = get_ac_present(data)
            if is_charging and not WAS_CHARGING_AC:
                if ac_w > AC_WATTS_THRESHOLD:
                    send_telegram(f"⚡ Llegó la corriente: la Delta 2 empezó a cargar por AC ({ac_w} W).")
                else:
                    send_telegram("⚡ Llegó la corriente (la batería ya está llena, no está cargando neto).")
                log.info("Notificado inicio de AC (%s W)", ac_w)
                LAST_AC_TIMESTAMP = time.time()
            elif not is_charging and WAS_CHARGING_AC:
                send_telegram("🔌⚠️ Se fue la luz: la Delta 2 dejó de tener AC conectado.")
                log.info("Notificado corte de luz (AC desconectado)")

            soc = _pick(data, "bms_bmsStatus.f32ShowSoc", "bms_bmsStatus.soc", "pd.soc")
            state_changed = is_charging != WAS_CHARGING_AC
            WAS_CHARGING_AC = is_charging

            if soc is not None:
                is_below = soc < BATTERY_LOW_THRESHOLD
                if is_below and not WAS_BELOW_LOW_THRESHOLD:
                    send_telegram(f"🪫 La carga bajó de {BATTERY_LOW_THRESHOLD}% (ahora {soc:.1f}%).")
                    log.info("Notificada carga baja (%.1f%%)", soc)
                if is_below != WAS_BELOW_LOW_THRESHOLD:
                    WAS_BELOW_LOW_THRESHOLD = is_below
                    state_changed = True

                is_full = soc >= FULL_CHARGE_THRESHOLD
                if is_full and not WAS_FULL:
                    send_telegram(f"🔋 La Delta 2 terminó de cargar ({soc:.1f}%).")
                    log.info("Notificada carga completa (%.1f%%)", soc)
                if is_full and not WAS_FULL:
                    WAS_FULL = True
                    state_changed = True
                elif not is_full and soc < FULL_CHARGE_RESET_THRESHOLD and WAS_FULL:
                    WAS_FULL = False
                    state_changed = True

            if state_changed:
                _save_persisted_state()
        except Exception:
            log.exception("Error chequeando carga por AC")


WATCHDOG_CHECK_MINUTES = 5
WATCHDOG_STALE_MINUTES = 5  # sin datos frescos por más de esto = alerta


def watchdog_timer() -> None:
    """Avisa si dejamos de recibir datos del dispositivo por MQTT (conexión
    caída, credenciales vencidas, etc.) — sin esto, un corte silencioso solo
    se nota cuando el usuario nota que dejaron de llegar informes."""
    global _DATA_STALE_ALERTED
    while True:
        time.sleep(WATCHDOG_CHECK_MINUTES * 60)
        if not ECOFLOW_READY or not USE_PRIVATE_API:
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


# --- Gestión de cargas: mensaje aparte del informe, cada 30 min entre 6:00 y
# 19:30, que dice qué debería estar encendido/apagado según el bloque horario
# del plan (que cubre las 24 h: la noche solo se consulta por /cargas, el
# timer automático no manda mensajes fuera de esa ventana). El frío
# (congelador) se eliminó del sistema — la NEVERA tomó su rol: es la carga
# protegida, se mantiene ON siempre salvo emergencia de batería, y se apaga
# programado a las 12 AM (aguanta cerrada hasta el amanecer). Ya no hay
# ninguna carga que se apague por exceso de consumo salvo la TV — es la
# única "gestionable" que queda, con dos ventanas de criterio distinto:
# mediodía (exceso real de consumo) y 6:30-7:30 PM (batería sobrada al 75%).
LOAD_ADVISOR_START_MIN = 6 * 60
LOAD_ADVISOR_END_MIN = 24 * 60  # el timer automatico llega hasta las 12 AM
BATTERY_EMERGENCY_THRESHOLD = 25  # debajo de esto, prioridad estricta: internet > nevera > resto

# El único horario fijo que queda es el apagado programado de la nevera a
# medianoche (es la carga protegida, aguanta cerrada hasta el amanecer).
# Laptop, TV, power bank y ventilador ya NO tienen ventanas horarias: se
# habilitan o no según el excedente real del sistema (system_net_w) en el
# momento de la consulta, sin importar la hora que sea.
_LOAD_SCHEDULE = [
    {"start": 6 * 60, "end": 7 * 60, "label": "6:00–7:00 AM", "nevera": "on",
     "battery_goal": "Sin sol aún, ~-2%"},
    {"start": 7 * 60, "end": 9 * 60, "label": "7:00–9:00 AM", "nevera": "on",
     "battery_goal": "Sube lento con el primer sol"},
    {"start": 9 * 60, "end": 12 * 60, "label": "9:00 AM–12:00 PM", "nevera": "on",
     "battery_goal": "~55-60% al mediodía"},
    {"start": 12 * 60, "end": 15 * 60, "label": "12:00–3:00 PM", "nevera": "on",
     "battery_goal": "65–75% a las 3 PM"},
    {"start": 15 * 60, "end": 16 * 60 + 30, "label": "3:00–4:30 PM", "nevera": "on",
     "battery_goal": "Mantener con el sol restante"},
    {"start": 16 * 60 + 30, "end": 18 * 60 + 30, "label": "4:30–6:30 PM", "nevera": "on",
     "battery_goal": "Cerca del 100% si se mantuvo solo nevera/internet"},
    {"start": 18 * 60 + 30, "end": 19 * 60 + 30, "label": "6:30–7:30 PM", "nevera": "on",
     "battery_goal": "Cerrar el día en 100%"},
    {"start": 19 * 60 + 30, "end": 24 * 60, "label": "7:30 PM–12:00 AM", "nevera": "on",
     "battery_goal": "Bajando controlado"},
    {"start": 0, "end": 6 * 60, "label": "12:00–6:00 AM", "nevera": "off_midnight",
     "battery_goal": "Amanecer con 25-40%"},
]


def _current_load_block(now=None) -> dict:
    now = now or datetime.now(TZ)
    minute_of_day = now.hour * 60 + now.minute
    for block in _LOAD_SCHEDULE:
        if block["start"] <= minute_of_day < block["end"]:
            return block
    return None


def _status_line(emoji: str, label: str, plan_ok: bool, device_keys: list, detail: str = "") -> str:
    """Une los dos conceptos que antes se confundían bajo el mismo 'ON/OFF':
    el semáforo (🟢/🔴) es lo que dice el PLAN (¿se puede tener encendido
    ahora?), y el texto ON/OFF es lo que vos marcaste de verdad con
    /on-/off — son cosas distintas y pueden no coincidir (ej. 🔴 ON = el plan
    dice que había que apagarlo pero lo tenés marcado prendido). Para nevera,
    laptop y TV (una sola unidad cada una)."""
    dot = "🟢" if plan_ok else "🔴"
    state_text = "ON" if DEVICE_STATE.get(device_keys[0]) else "OFF"
    line = f"{emoji} {label}: {dot} {state_text}"
    if detail:
        line += f" ({detail})"
    return line


def _multi_unit_line(emoji: str, label: str, device_keys: list, available_w) -> tuple:
    """Para power bank / ventilador (varias unidades, cada una con
    su propio watiaje): un punto 🟢/🔴 por unidad, según si esa unidad
    puntual entra en el excedente disponible ahora mismo (orden acumulativo:
    la primera que no entra dice 🔴, y de ahí en más también, aunque
    individualmente pesen menos — es la lógica de "qué te podés ir
    permitiendo en orden"). El marcado real (/on-/off) va pegado a cada
    dot con un ✓ — son datos distintos (uno es "cuánto aguanta el
    sistema", el otro es "qué tenés prendido de verdad") pero van juntos
    en vez de un conteo aparte. Si TODAS las unidades están marcadas, se
    resume con un solo ✅ al final en vez de repetir el ✓ en cada una.

    El detalle de watts (mismo estilo que laptop/TV: "necesitas X W,
    tienes Y W") solo se muestra si hace falta actuar: alguna unidad
    marcada ON de verdad está en rojo. X es la suma de TODAS las unidades
    marcadas (lo que pediste en total), no solo las que no entraron.

    `available_w` ya viene descontado de lo que se llevaron las cargas de
    mayor prioridad (ver _allocate_budget) — no es el excedente total del
    sistema, es lo que queda para ESTA carga en particular."""
    original_available = max(0, available_w) if available_w is not None else 0
    remaining = original_available
    dots = []
    for key in device_keys:
        watts = DEVICE_INFO[key]["watts"]
        if watts <= remaining:
            dots.append("🟢")
            remaining -= watts
        else:
            dots.append("🔴")
    on_count = sum(1 for k in device_keys if DEVICE_STATE.get(k))
    if on_count == len(device_keys):
        dots_str = " ".join(dots) + " ✅"
    else:
        dots_str = " ".join(
            f"{dot}✓" if DEVICE_STATE.get(key) else dot
            for dot, key in zip(dots, device_keys)
        )
    line = f"{emoji} {label}: {dots_str}"
    actionable = any(DEVICE_STATE.get(key) and dot == "🔴" for key, dot in zip(device_keys, dots))
    if actionable:
        needed = sum(DEVICE_INFO[key]["watts"] for key in device_keys if DEVICE_STATE.get(key))
        line += f" — necesitas {needed} W, tienes {round(original_available)} W"
    return line, remaining


def _allocate_budget(watts: int, available_w) -> tuple:
    """Descuenta `watts` del excedente disponible si entra, y devuelve
    (ok, detail, excedente_restante) — la pieza que permite que Laptop, TV,
    Power bank y Ventilador se repartan el MISMO excedente en vez de que
    cada uno lo evalúe por separado contra el total (eso hacía que
    aparecieran varias en verde a la vez aunque juntas no entraran)."""
    if available_w is None:
        return True, "", None
    remaining = max(0, available_w)
    if watts <= remaining:
        return True, "", remaining - watts
    return False, f"necesitas {watts} W, tienes {round(remaining)} W", remaining


def _nevera_status(nevera_mode: str) -> tuple:
    """La nevera es la carga protegida (tomó el rol que tenía el frío): el
    plan la da por buena siempre salvo el apagado programado a las 12 AM
    (aguanta cerrada hasta el amanecer). El caso de emergencia de batería se
    maneja aparte en build_load_advisor_message (ahí apaga todo)."""
    if nevera_mode == "off_midnight":
        return False, "aguanta cerrada hasta el amanecer"
    return True, ""


POWERBANK_DEVICE_KEYS = [f"powerbank{i}" for i in range(1, MULTI_UNIT_DEVICES["powerbank"][0] + 1)]
VENTILADOR_DEVICE_KEYS = [f"ventilador{i}" for i in range(1, MULTI_UNIT_DEVICES["ventilador"][0] + 1)]


def build_load_advisor_message(m: dict = None) -> str:
    """Nevera, Laptop, TV, Power bank y Ventilador — cada una ya evalúa el
    estado real (watts, batería) en vez de ser un texto fijo. Internet no se
    muestra: es fija, siempre ON, no hay nada que decidir ni informar ahí.
    Prioridad: Internet > Nevera (protegidas, no compiten por excedente) >
    Laptop > Ventilador > Power bank > TV — estas cuatro últimas se reparten
    el MISMO excedente (system_net_w) en orden, restando lo que cada una se
    lleva antes de evaluar la siguiente. Antes cada una miraba el excedente
    total por separado, lo que podía mostrar varias en verde a la vez aunque
    juntas no entraran. Por debajo de BATTERY_EMERGENCY_THRESHOLD se apaga
    TODO menos internet — no alcanza con bajar solo la nevera si el resto
    sigue mostrando 'ON' como si nada, porque son de menor prioridad y deben
    ceder primero/junto con ella."""
    block = _current_load_block()
    if block is None:
        return ""
    now = datetime.now(TZ)
    if m is None:
        m = _gather_metrics()
    avg_soc_str = f"{m['avg_soc']:.1f}%" if m["avg_soc"] is not None else "N/D"
    emergency = block["nevera"] != "off_midnight" and m["avg_soc"] is not None and m["avg_soc"] < BATTERY_EMERGENCY_THRESHOLD

    if emergency:
        vent_line, _ = _multi_unit_line("🌀", "Ventilador", VENTILADOR_DEVICE_KEYS, 0)
        pb_line, _ = _multi_unit_line("🔋", "Power bank", POWERBANK_DEVICE_KEYS, 0)
        lines = [
            f"🔆 *Gestión de cargas* · {block['label']}",
            f"🚨 EMERGENCIA DE BATERÍA — {avg_soc_str}, apagar todo menos internet",
            _status_line("🥶", "Nevera", False, ["nevera"]),
            _status_line("💻", "Laptop", False, ["laptop"]),
            vent_line,
            pb_line,
            _status_line("📺", "TV", False, ["tv"]),
            "",
            f"🎯 Meta: {block['battery_goal']} (ahora {avg_soc_str})",
        ]
    else:
        nevera_ok, nevera_detail = _nevera_status(block["nevera"])

        available = m["system_net_w"]
        laptop_ok, laptop_detail, available = _allocate_budget(DEVICE_INFO["laptop"]["watts"], available)
        if laptop_ok or not DEVICE_STATE.get("laptop"):
            laptop_detail = ""  # sin acción pendiente: ya está OFF, no hace falta el numero
        laptop_line = _status_line("💻", "Laptop", laptop_ok, ["laptop"], laptop_detail)

        vent_line, available = _multi_unit_line("🌀", "Ventilador", VENTILADOR_DEVICE_KEYS, available)
        pb_line, available = _multi_unit_line("🔋", "Power bank", POWERBANK_DEVICE_KEYS, available)

        tv_ok, tv_detail, available = _allocate_budget(DEVICE_INFO["tv"]["watts"], available)
        if tv_ok or not DEVICE_STATE.get("tv"):
            tv_detail = ""
        tv_line = _status_line("📺", "TV", tv_ok, ["tv"], tv_detail)

        lines = [
            f"🔆 *Gestión de cargas* · {block['label']}",
            _status_line("🥶", "Nevera", nevera_ok, ["nevera"], nevera_detail),
            laptop_line,
            vent_line,
            pb_line,
            tv_line,
            "",
            f"🎯 Meta: {block['battery_goal']} (ahora {avg_soc_str})",
        ]
    proj = _project_to_checkpoint(now, m["avg_soc"], m["system_net_w"])
    if proj:
        proj_label, proj_floor, proj_pct = proj
        if proj_pct >= proj_floor:
            lines.append(f"✅ SE VA A CUMPLIR la meta: proyectás {proj_pct:.0f}% para {proj_label} (meta {proj_floor}%+)")
        else:
            lines.append(f"⚠️ NO SE VA A CUMPLIR la meta: proyectás {proj_pct:.0f}% para {proj_label} (meta {proj_floor}%+)")
    weak_charge = _weak_charge_note(m["pv_w"], m["delta2_net_w"], m["extra_net_w"])
    if weak_charge:
        lines.append(weak_charge)
    return "\n".join(lines)


WEAK_CHARGE_MIN_PV_W = 100  # sol mínimo para que tenga sentido evaluar el ritmo de carga
WEAK_CHARGE_RATE_W = 30  # por debajo de esto, con sol disponible, se considera carga débil


def _weak_charge_note(pv_w, delta2_net_w, extra_net_w):
    """La prioridad real es la carga en sí (sin carga no hay nada que
    gestionar) — esto chequea si la batería externa está cargando a buen
    ritmo cuando hay sol de sobra, no solo si el signo es positivo. Una
    batería que "carga" a +5 W con 300 W de sol disponible tiene un
    problema real (mala conexión, límite del BMS, etc.) que el
    total/promedio combinado no deja ver."""
    if pv_w is None or pv_w < WEAK_CHARGE_MIN_PV_W:
        return None
    if extra_net_w is not None and 0 < extra_net_w < WEAK_CHARGE_RATE_W:
        return "🐌 Carga débil de la batería externa"
    return None




# --- Alerta dinámica de proyección: a diferencia del mensaje de /cargas (que
# describe el plan), esto analiza el ritmo de descarga actual y proyecta si la
# batería va a llegar por debajo de la meta del próximo checkpoint del plan
# (10% al amanecer, ~55-60% al mediodía, 65-75% a las 3 PM, 100% al cierre
# del día — este último solo es realista con nevera+internet nomás). Los
# checkpoints son cíclicos: si ya pasaron todos los de hoy,
# se proyecta contra el primero de mañana (el amanecer), cruzando la
# medianoche — así de 8 PM en adelante el mensaje evalúa si vas a sobrevivir
# la noche, no se queda mudo hasta el mediodía siguiente. Avisa ANTES de que
# pase, no cuando ya se descontroló. Chequea más seguido que el mensaje de
# 30 min para reaccionar rápido a caídas bruscas.
PROJECTION_CHECK_MINUTES = 10
PROJECTION_ALERT_MARGIN = 5  # puntos porcentuales por debajo de la meta para disparar la alerta
BATTERY_CHECKPOINTS = [
    (6 * 60, 10, "el amanecer"),
    (12 * 60, 55, "el mediodía"),
    (15 * 60, 65, "las 3:00 PM"),
    (19 * 60 + 30, 100, "el cierre del día (7:30 PM)"),
]  # ordenados por hora del día — el orden importa para _next_checkpoint. El
# 100% del cierre solo es realista si te mantenés en nevera+internet nomás
# (laptop/TV/ventilador/power bank se comen el excedente que hace falta
# para juntar esa carga) — confirmado con el usuario, no es un objetivo
# válido bajo uso mixto normal.

_projection_alerted_for = {}  # {checkpoint_min: date} último día que ya se avisó ese checkpoint


def _next_checkpoint(now):
    minute_of_day = now.hour * 60 + now.minute
    for cp_min, floor, label in BATTERY_CHECKPOINTS:
        if minute_of_day < cp_min:
            return cp_min, floor, label
    # ya pasaron todos los de hoy: el próximo es el primero de mañana (el amanecer)
    return BATTERY_CHECKPOINTS[0]


def _project_to_checkpoint(now, avg_soc, system_net_w):
    """Proyecta el %SOC combinado (Delta 2 + batería extra) en el próximo
    checkpoint del plan al ritmo de descarga actual. Devuelve
    (label, floor, projected) o None si falta algún dato. El cálculo de
    horas restantes cruza la medianoche si el checkpoint es de mañana (ej.
    el amanecer, evaluado desde la noche anterior). Lo usan tanto la alerta
    dinámica como la línea de proyección del mensaje de /cargas."""
    cp = _next_checkpoint(now)
    if cp is None or avg_soc is None or system_net_w is None:
        return None
    cp_min, floor, label = cp
    minute_of_day = now.hour * 60 + now.minute
    minutes_left = (cp_min - minute_of_day) % (24 * 60)
    hours_left = minutes_left / 60
    capacity_wh = BATTERY_CAPACITY_WH * 2
    projected = avg_soc + (system_net_w * hours_left) / capacity_wh * 100
    return label, floor, max(0.0, min(100.0, projected))


def _check_battery_projection(now=None) -> None:
    """Un chequeo: proyecta el %SOC en el próximo checkpoint con el ritmo de
    descarga actual y avisa (una vez por checkpoint/día, con rearme si se
    recupera) si va a quedar por debajo de la meta menos el margen."""
    now = now or datetime.now(TZ)
    minute_of_day = now.hour * 60 + now.minute
    if not (LOAD_ADVISOR_START_MIN <= minute_of_day < LOAD_ADVISOR_END_MIN):
        return
    m = _gather_metrics()
    proj = _project_to_checkpoint(now, m["avg_soc"], m["system_net_w"])
    if proj is None:
        return
    label, floor, projected = proj
    cp_min, _, _ = _next_checkpoint(now)
    today = now.date()
    if m["system_net_w"] >= -NOISE_FLOOR_W:
        # no está descargando neto ahora mismo: sin riesgo, se puede rearmar
        # la alerta si se había disparado antes y se recuperó
        if _projection_alerted_for.get(cp_min) == today:
            del _projection_alerted_for[cp_min]
        return
    if projected < floor - PROJECTION_ALERT_MARGIN:
        if _projection_alerted_for.get(cp_min) != today:
            send_telegram(
                "⚠️ *Se está yendo de control*\n"
                f"Proyecto {projected:.0f}% para {label} (meta {floor}%+), vas en {m['avg_soc']:.1f}% "
                f"descargando a {round(m['system_net_w'])} W. Bajá carga (nevera, TV, laptop)."
            )
            _projection_alerted_for[cp_min] = today
            log.info("Alerta de proyección de batería enviada (checkpoint %s)", label)
    elif _projection_alerted_for.get(cp_min) == today:
        del _projection_alerted_for[cp_min]


def battery_projection_timer() -> None:
    while True:
        time.sleep(PROJECTION_CHECK_MINUTES * 60)
        if not ECOFLOW_READY:
            continue
        try:
            _check_battery_projection()
        except Exception:
            log.exception("Error en el chequeo de proyección de batería")


# --- Dashboard web: la misma info que el bot, en vivo, sin tener que pedirla ---
PORT = int(os.environ.get("PORT", "8080"))


def get_device_state_payload() -> dict:
    return {
        "devices": [
            {"key": key, "label": info["label"], "emoji": info["emoji"], "watts": info["watts"], "on": DEVICE_STATE[key]}
            for key, info in DEVICE_INFO.items()
        ]
    }


def get_dashboard_status() -> dict:
    if not ECOFLOW_READY:
        return {"ready": False, "error": "EcoFlow todavía no está configurado"}
    try:
        m = _gather_metrics(passive=True)
    except Exception as exc:
        return {"ready": False, "error": str(exc)}

    percent = m["avg_soc"] if m["avg_soc"] is not None else m["soc_delta2"]

    eta_text = None
    eta_ok = None
    remain_duration = None
    is_stable = bool(m["remain"] and m["remain"]["stable"])
    if is_stable:
        remain_duration = "—"
    elif m["remain"]:
        r = m["remain"]
        eta_ok = r["charging_up"]
        eta_text = f"Llena a las {r['eta']}" if eta_ok else f"Dura hasta las {r['eta']}"
        remain_duration = f"{r['hours']}h {r['minutes']}m"

    return {
        "ready": True,
        "percent": percent,
        "soc_delta2": m["soc_delta2"],
        "soc_extra": m["soc_extra"],
        "source_verb": m["source_verb"],
        "source_emoji": m["source_emoji"],
        "pv_w": m["pv_w"] or 0,
        "ac_w": m["ac_w"] or 0,
        "delta2_net_w": round(m["delta2_net_w"]) if m["delta2_net_w"] is not None else None,
        "extra_net_w": round(m["extra_net_w"]) if m["extra_net_w"] is not None else None,
        "has_ac": m["has_ac"],
        "in_w": m["total_in_w"],
        "out_w": m["out_w"],
        "eta_text": eta_text,
        "eta_ok": eta_ok,
        "remain_duration": remain_duration,
        "threshold_text": m["threshold_line"].replace("🪫 ", "") if m["threshold_line"] else None,
        "threshold_pct": BATTERY_LOW_THRESHOLD,
        "last_ac_text": _last_ac_line().replace("⚡ ", ""),
        "ports": m["ports"],
        "updated_at": datetime.now(TZ).strftime("%H:%M:%S"),
    }


DASHBOARD_HTML = """<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<title>EcoFlow</title>
<style>
  * { box-sizing: border-box; }
  :root { --ease-out: cubic-bezier(0.23, 1, 0.32, 1); }
  body {
    margin: 0; min-height: 100vh; background: #0b0f14; color: #f5f5f5;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    display: flex; flex-direction: column; align-items: center; padding: 20px 16px 50px;
  }
  .io-row {
    width: 100%; max-width: 380px; display: flex; justify-content: space-between;
    align-items: flex-start; margin-bottom: 18px;
  }
  .io-col .io-label { font-size: 13px; color: #6b7684; transition: color 200ms var(--ease-out); display: flex; align-items: center; gap: 4px; }
  .io-col.out .io-label { justify-content: flex-end; }
  .io-col .io-label.active { color: #4ade80; }
  .io-col.out .io-label.active { color: #f87171; }
  .io-svg { flex-shrink: 0; }
  .io-col .io-value { font-size: 20px; font-weight: 600; margin-top: 2px; font-variant-numeric: tabular-nums; }
  .io-col.out { text-align: right; }
  .io-center { text-align: center; padding-top: 4px; }
  .io-center .verb { font-size: 13px; color: #9aa4af; }
  .io-center .emoji { font-size: 22px; margin-top: 2px; }
  .icons-row { width: 100%; max-width: 380px; display: flex; justify-content: space-around; margin-bottom: 14px; }
  .icon-item { display: flex; flex-direction: column; align-items: center; width: 84px; }
  .icon-name { font-size: 10px; color: #6b7684; margin-top: 2px; letter-spacing: .3px; text-transform: uppercase; }
  .icon-circle {
    width: 52px; height: 52px; border-radius: 50%; background: #1c232b;
    display: flex; align-items: center; justify-content: center; font-size: 22px;
    transition: background-color 200ms var(--ease-out);
  }
  .icon-circle.charging { background-color: #14351f; }
  .icon-circle.discharging { background-color: #3a1616; }
  .icon-watts { font-size: 12px; color: #9aa4af; margin-top: 5px; font-variant-numeric: tabular-nums; }
  .icon-dir { font-size: 11px; margin-top: 1px; transition: color 200ms var(--ease-out); }
  .icon-dir.charging { color: #4ade80; }
  .icon-dir.discharging { color: #f87171; }
  .batt-icon { display: inline-flex; align-items: center; transition: color 200ms var(--ease-out); }
  .batt-icon.batt-green { color: #4ade80; }
  .batt-icon.batt-red { color: #f87171; }
  .batt-icon.batt-gray { color: #6b7684; }
  .ring-wrap { position: relative; width: 240px; height: 240px; margin: 6px 0 8px; }
  .ring {
    width: 100%; height: 100%; border-radius: 50%;
    background: conic-gradient(var(--ring-color, #22c55e) calc(var(--pct, 0) * 1%), #1c232b 0);
    display: flex; align-items: center; justify-content: center;
    transition: background 250ms var(--ease-out);
  }
  .ring-inner {
    width: 80%; height: 80%; border-radius: 50%; background: #0b0f14;
    display: flex; flex-direction: column; align-items: center; justify-content: center;
  }
  .pct { font-size: 48px; font-weight: 700; line-height: 1; font-variant-numeric: tabular-nums; }
  .pct-sub { font-size: 13px; color: #9aa4af; margin-top: 8px; text-align: center; }
  .pct-sub .dur { font-size: 22px; color: #e5e7eb; font-weight: 700; margin-top: 2px; font-variant-numeric: tabular-nums; }
  .eta-box {
    margin-top: 4px; padding: 14px 22px; border-radius: 16px; background: #141b22;
    text-align: center; max-width: 340px; width: 100%;
    opacity: 0; transform: scale(0.97); visibility: hidden;
    transition: opacity 200ms var(--ease-out), transform 200ms var(--ease-out), visibility 200ms;
  }
  .eta-box.visible { opacity: 1; transform: scale(1); visibility: visible; }
  .eta-box .eta-main { font-size: 22px; font-weight: 700; font-variant-numeric: tabular-nums; }
  .eta-main.eta-ok { color: #4ade80; }
  .eta-main.eta-warn { color: #f87171; }
  .eta-box .eta-sub { font-size: 13px; color: #9aa4af; margin-top: 4px; display: inline-flex; align-items: center; gap: 4px; justify-content: center; }
  .eta-box .eta-sub .batt-icon { width: 14px; }
  .batteries { width: 100%; max-width: 380px; margin-top: 16px; }
  .battery-row {
    display: flex; justify-content: space-between; align-items: center;
    background: #141b22; border-radius: 14px; padding: 12px 16px; margin-top: 8px;
  }
  .battery-row .name { font-size: 14px; color: #cbd5e1; display: flex; align-items: center; gap: 6px; }
  .battery-row .val { font-size: 16px; font-weight: 700; font-variant-numeric: tabular-nums; }
  .battery-row .val.charging { color: #4ade80; }
  .battery-row .val.discharging { color: #f87171; }
  .ports { width: 100%; max-width: 380px; margin-top: 14px; }
  .ports .title { font-size: 13px; color: #9aa4af; margin-bottom: 6px; }
  .port-row { display: flex; justify-content: space-between; align-items: center; font-size: 14px; padding: 4px 4px; color: #cbd5e1; }
  .port-row .port-name { display: flex; align-items: center; gap: 6px; }
  .usb-svg { flex-shrink: 0; color: #9aa4af; }
  .port-row span:last-child { font-variant-numeric: tabular-nums; }
  .devices { width: 100%; max-width: 380px; margin-top: 14px; }
  .devices .title { font-size: 13px; color: #9aa4af; margin-bottom: 6px; }
  .device-btn {
    display: flex; justify-content: space-between; align-items: center; width: 100%;
    font-size: 14px; padding: 8px 12px; margin-bottom: 6px; border-radius: 10px;
    background: #141b22; border: 1px solid #232b33; color: #cbd5e1; cursor: pointer;
    transition: background-color 150ms var(--ease-out), border-color 150ms var(--ease-out);
  }
  .device-btn .name { display: flex; align-items: center; gap: 8px; }
  .device-btn .state { font-weight: 700; font-size: 12px; letter-spacing: 0.03em; }
  .device-btn.on { border-color: #4ade8055; background: #1a2b1f; }
  .device-btn.on .state { color: #4ade80; }
  .device-btn.off .state { color: #6b7684; }
  .cargas { width: 100%; max-width: 380px; margin-top: 14px; }
  .cargas .title { font-size: 13px; color: #9aa4af; margin-bottom: 6px; }
  .cargas-box {
    font-size: 14px; line-height: 1.7; color: #cbd5e1; white-space: pre-line;
    background: #141b22; border: 1px solid #232b33; border-radius: 10px; padding: 12px 14px;
  }
  .updated { margin-top: 22px; font-size: 12px; color: #7b8794; }
  .live-dot {
    display: inline-block; width: 6px; height: 6px; border-radius: 50%;
    background: #6b7684; margin-right: 6px; vertical-align: middle;
    transition: background-color 300ms var(--ease-out);
  }
  .live-dot.ok { background: #4ade80; }
  .live-dot.stale { background: #ef4444; animation: pulse 1s ease-in-out infinite; }
  @keyframes pulse { 50% { opacity: 0.3; } }
</style>
</head>
<body>
  <div class="io-row">
    <div class="io-col in"><div class="io-label" id="in-label"><svg class="io-svg" viewBox="0 0 24 24" width="14" height="14"><path d="M12 3v10m0 0l-4-4m4 4l4-4M4 19h16" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg> Entrada</div><div class="io-value" id="in-w">-- W</div></div>
    <div class="io-center">
      <div class="verb" id="source-verb">Cargando…</div>
      <div class="emoji" id="source-emoji"></div>
    </div>
    <div class="io-col out"><div class="io-label" id="out-label">Salida <svg class="io-svg" viewBox="0 0 24 24" width="14" height="14"><path d="M12 21V11m0 0l-4 4m4-4l4 4M4 5h16" stroke="currentColor" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round"/></svg></div><div class="io-value" id="out-w">-- W</div></div>
  </div>

  <div class="icons-row">
    <div class="icon-item">
      <div class="icon-circle" id="ac-circle">🔌</div>
      <div class="icon-watts" id="ac-w">0 W</div>
      <div class="icon-dir" id="ac-status"></div>
      <div class="icon-name">AC</div>
    </div>
    <div class="icon-item">
      <div class="icon-circle" id="extra-circle">🔋</div>
      <div class="icon-watts" id="extra-w">0 W</div>
      <div class="icon-dir" id="extra-dir"></div>
      <div class="icon-name">Extra</div>
    </div>
    <div class="icon-item">
      <div class="icon-circle" id="pv-circle">☀️</div>
      <div class="icon-watts" id="pv-w">0 W</div>
      <div class="icon-name">Solar</div>
    </div>
  </div>

  <div class="ring-wrap">
    <div class="ring" id="ring">
      <div class="ring-inner">
        <div class="pct" id="pct">--%</div>
        <div class="pct-sub">Tiempo restante<div class="dur" id="dur">--</div></div>
      </div>
    </div>
  </div>

  <div class="eta-box" id="eta-box">
    <div class="eta-main" id="eta-main"></div>
    <div class="eta-sub" id="eta-sub"></div>
  </div>

  <div class="batteries" id="batteries"></div>

  <div class="ports" id="ports-wrap" style="display:none">
    <div class="title">Puertos activos</div>
    <div id="ports"></div>
  </div>

  <div class="cargas" id="cargas-wrap" style="display:none">
    <div class="title">Gestión de cargas</div>
    <div class="cargas-box" id="cargas-box"></div>
  </div>

  <div class="devices">
    <div class="title">Qué tenés encendido</div>
    <div id="devices"></div>
  </div>

  <div class="devices">
    <div class="title">Alerta de batería baja</div>
    <div style="display:flex;align-items:center;gap:8px;padding:8px 0">
      <input type="number" id="alerta-input" min="0" max="100" style="width:70px;padding:6px 8px;border-radius:8px;border:1px solid #333;background:#111;color:#fff;font-size:15px">
      <span>%</span>
      <button id="alerta-save" style="padding:6px 14px;border-radius:8px;border:none;background:#3b82f6;color:#fff;font-size:14px;cursor:pointer">Guardar</button>
      <span id="alerta-msg" style="font-size:13px;color:#22c55e"></span>
    </div>
  </div>

  <div class="updated">
    <span class="live-dot" id="live-dot"></span>
    <span id="updated-text"></span>
  </div>
  <script>
    // Ícono de batería en SVG (verde=carga, roja=descarga, gris=estable/sin datos).
    // Un solo shape, coloreado con currentColor según la clase, en vez de 3
    // archivos separados — mismo resultado visual, menos código repetido.
    const BATTERY_SVG = `<svg viewBox="0 0 28 16" width="26" height="15">
      <rect x="1" y="1" width="23" height="14" rx="3" fill="none" stroke="currentColor" stroke-width="2"/>
      <rect x="25" y="5.5" width="2.5" height="5" rx="1" fill="currentColor"/>
      <rect x="3.5" y="3.5" width="18" height="9" rx="1.5" fill="currentColor"/>
    </svg>`;
    function batteryIcon(state) {
      // state: 'charging' | 'discharging' | 'neutral'
      const cls = state === 'charging' ? 'batt-green' : state === 'discharging' ? 'batt-red' : 'batt-gray';
      return `<span class="batt-icon ${cls}">${BATTERY_SVG}</span>`;
    }

    // Ícono de puerto USB-C: forma ovalada típica del conector
    const USB_SVG = `<svg class="usb-svg" viewBox="0 0 24 12" width="18" height="9">
      <rect x="1" y="1" width="22" height="10" rx="5" fill="none" stroke="currentColor" stroke-width="2"/>
    </svg>`;

    function batteryFlow(netW) {
      // mismo criterio que el informe de Telegram: neutral (gris) o carga (verde)/descarga (rojo)
      if (netW == null || (netW > -5 && netW < 5)) return { state: 'neutral', label: 'Carga', suffix: '', cls: '' };
      if (netW > 5) return { state: 'charging', label: 'Carga', suffix: ` (${Math.round(netW)} W)`, cls: 'charging' };
      return { state: 'discharging', label: 'Descarga', suffix: ` (${Math.abs(Math.round(netW))} W)`, cls: 'discharging' };
    }

    let reqId = 0;
    async function refresh() {
      const myId = ++reqId;
      try {
        const res = await fetch('/api/status');
        const d = await res.json();
        if (myId !== reqId) return; // llegó una respuesta vieja después de una más nueva, se descarta
        if (!d.ready) {
          document.getElementById('source-verb').textContent = d.error || 'No listo todavía';
          document.getElementById('live-dot').classList.remove('ok');
          document.getElementById('live-dot').classList.add('stale');
          return;
        }
        document.getElementById('source-verb').textContent = d.source_verb;
        document.getElementById('source-emoji').textContent = d.source_emoji;

        const pct = d.percent != null ? d.percent : 0;
        const ring = document.getElementById('ring');
        ring.style.setProperty('--pct', pct);
        ring.style.setProperty('--ring-color', pct <= 10 ? '#ef4444' : pct <= 20 ? '#eab308' : '#22c55e');
        document.getElementById('pct').textContent = (d.percent != null ? d.percent.toFixed(1) : '--') + '%';
        document.getElementById('dur').textContent = d.remain_duration || '--';

        const etaBox = document.getElementById('eta-box');
        if (d.eta_text) {
          etaBox.classList.add('visible');
          const etaMain = document.getElementById('eta-main');
          etaMain.textContent = d.eta_text;
          etaMain.className = 'eta-main ' + (d.eta_ok ? 'eta-ok' : 'eta-warn');
          const etaSub = document.getElementById('eta-sub');
          if (d.threshold_text) {
            etaSub.innerHTML = batteryIcon('discharging') + d.threshold_text;
          } else {
            etaSub.textContent = d.last_ac_text || '';
          }
        } else {
          etaBox.classList.remove('visible');
        }

        const alertaInput = document.getElementById('alerta-input');
        if (d.threshold_pct != null && document.activeElement !== alertaInput) {
          alertaInput.value = d.threshold_pct;
        }

        document.getElementById('in-w').textContent = (d.in_w ?? '--') + ' W';
        document.getElementById('out-w').textContent = (d.out_w ?? '--') + ' W';
        document.getElementById('ac-w').textContent = (d.ac_w ?? '--') + ' W';
        const acCircle = document.getElementById('ac-circle');
        const acStatus = document.getElementById('ac-status');
        acCircle.className = 'icon-circle' + (d.has_ac ? ' charging' : '');
        acStatus.textContent = d.has_ac ? 'Sí' : 'No';
        acStatus.className = 'icon-dir' + (d.has_ac ? ' charging' : '');
        document.getElementById('pv-w').textContent = (d.pv_w ?? '--') + ' W';
        document.getElementById('pv-circle').className = 'icon-circle' + (d.pv_w > 5 ? ' charging' : '');
        document.getElementById('in-label').classList.toggle('active', d.in_w > 0);
        document.getElementById('out-label').classList.toggle('active', d.out_w > 0);

        // Batería extra: mismo criterio que las filas de abajo (batteryFlow), sin duplicar la lógica
        const extraCircle = document.getElementById('extra-circle');
        const extraDir = document.getElementById('extra-dir');
        const extraNet = d.extra_net_w;
        const ef = batteryFlow(extraNet);
        document.getElementById('extra-w').textContent = (extraNet == null ? '--' : Math.abs(extraNet)) + ' W';
        extraCircle.innerHTML = batteryIcon(ef.state);
        extraCircle.className = 'icon-circle ' + ef.cls;
        extraDir.textContent = ef.state === 'charging' ? '↑ carga' : ef.state === 'discharging' ? '↓ descarga' : '';
        extraDir.className = 'icon-dir ' + ef.cls;

        let batHtml = '';
        if (d.soc_delta2 != null) {
          const f = batteryFlow(d.delta2_net_w);
          batHtml += `<div class="battery-row"><div class="name">${batteryIcon(f.state)} Delta 2 — ${f.label}</div><div class="val ${f.cls}">${d.soc_delta2.toFixed(1)}%${f.suffix}</div></div>`;
        }
        if (d.soc_extra != null) {
          const f = batteryFlow(d.extra_net_w);
          batHtml += `<div class="battery-row"><div class="name">${batteryIcon(f.state)} Batería Extra — ${f.label}</div><div class="val ${f.cls}">${d.soc_extra.toFixed(1)}%${f.suffix}</div></div>`;
        }
        document.getElementById('batteries').innerHTML = batHtml;

        const portsWrap = document.getElementById('ports-wrap');
        if (d.ports && d.ports.length) {
          portsWrap.style.display = 'block';
          document.getElementById('ports').innerHTML = d.ports.map(
            p => `<div class="port-row"><span class="port-name">${USB_SVG}${p.name}</span><span>${p.watts} W</span></div>`
          ).join('');
        } else {
          portsWrap.style.display = 'none';
        }

        document.getElementById('live-dot').classList.remove('stale');
        document.getElementById('live-dot').classList.add('ok');
        lastSuccessAt = Date.now();
      } catch (e) {
        document.getElementById('live-dot').classList.add('stale');
      }
    }

    // Contador de "desactualizado hace Xm" que sigue corriendo aunque el
    // fetch falle (no depende de que el servidor responda para contar).
    let lastSuccessAt = null;
    function tickClock() {
      const updatedText = document.getElementById('updated-text');
      if (lastSuccessAt == null) {
        updatedText.textContent = 'Conectando…';
        return;
      }
      const secs = Math.round((Date.now() - lastSuccessAt) / 1000);
      if (secs < 3) {
        updatedText.textContent = 'Actualizado ahora';
      } else if (secs < 60) {
        updatedText.textContent = `Actualizado hace ${secs}s`;
      } else {
        updatedText.textContent = `Desactualizado hace ${Math.round(secs / 60)}m`;
      }
    }

    // Panel de dispositivos: se refresca aparte (mucho menos seguido que el
    // estado del EcoFlow) y solo vuelve a pedir datos cuando el usuario
    // togglea algo, para no pisar un click en curso con un refresh automático.
    async function loadDevices() {
      try {
        const res = await fetch('/api/devices');
        const d = await res.json();
        renderDevices(d.devices);
      } catch (e) { /* silencioso, no es crítico como el estado del EcoFlow */ }
    }

    function renderDevices(devices) {
      document.getElementById('devices').innerHTML = devices.map(dev => `
        <div class="device-btn ${dev.on ? 'on' : 'off'}" data-key="${dev.key}">
          <span class="name">${dev.emoji} ${dev.label} · ${dev.watts}W</span>
          <span class="state">${dev.on ? 'ON' : 'OFF'}</span>
        </div>
      `).join('');
      document.querySelectorAll('.device-btn').forEach(btn => {
        btn.addEventListener('click', async () => {
          const key = btn.dataset.key;
          const turningOn = !btn.classList.contains('on');
          try {
            const res = await fetch('/api/devices', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ device: key, on: turningOn }),
            });
            const d = await res.json();
            if (d.devices) renderDevices(d.devices);
          } catch (e) { /* si falla, el próximo loadDevices() corrige la vista */ }
        });
      });
    }

    // Mismo texto que manda el bot por Telegram (build_load_advisor_message),
    // solo se reformatea el *negrita* de Telegram a <strong> para HTML.
    function escapeHtml(s) {
      return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }
    async function loadCargas() {
      try {
        const res = await fetch('/api/cargas');
        const d = await res.json();
        const wrap = document.getElementById('cargas-wrap');
        if (d.message) {
          wrap.style.display = 'block';
          document.getElementById('cargas-box').innerHTML =
            escapeHtml(d.message).replace(/\\*(.+?)\\*/g, '<strong>$1</strong>');
        } else {
          wrap.style.display = 'none';
        }
      } catch (e) { /* silencioso, no es crítico como el estado del EcoFlow */ }
    }

    document.getElementById('alerta-save').addEventListener('click', async () => {
      const msg = document.getElementById('alerta-msg');
      const pct = parseInt(document.getElementById('alerta-input').value, 10);
      if (isNaN(pct) || pct < 0 || pct > 100) {
        msg.style.color = '#ef4444';
        msg.textContent = 'Poné un número entre 0 y 100';
        return;
      }
      try {
        const res = await fetch('/api/alerta', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ threshold_pct: pct }),
        });
        if (!res.ok) throw new Error();
        msg.style.color = '#22c55e';
        msg.textContent = 'Guardado ✓';
        setTimeout(() => { msg.textContent = ''; }, 2000);
      } catch (e) {
        msg.style.color = '#ef4444';
        msg.textContent = 'No se pudo guardar';
      }
    });

    refresh();
    loadDevices();
    loadCargas();
    setInterval(refresh, 3000);
    setInterval(tickClock, 1000);
    setInterval(loadDevices, 3000);
    setInterval(loadCargas, 3000);
  </script>
</body>
</html>
"""


class _DashboardHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # ya logueamos lo importante aparte; esto evita ruido por cada poll

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        # Sin esto, el navegador (o Telegram WebView) puede quedarse con un HTML
        # viejo mientras la API ya cambió de forma — como pasó una vez con
        # extra_in_w/extra_out_w renombrados a extra_net_w ("undefined W" en pantalla).
        self.send_header("Cache-Control", "no-store")
        # Sin esto, cualquier front separado (la app de Expo en Vercel, por
        # ejemplo) que llame a esta API desde otro dominio se queda bloqueada
        # por CORS del lado del navegador — la app nativa no lo sufre (no
        # aplica CORS), por eso pasaba desapercibido hasta que se desplegó la
        # versión web. No hay sesión ni cookies acá, así que "*" es seguro.
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        # Preflight de CORS para el POST de /api/devices (lleva Content-Type:
        # application/json, así que el navegador manda OPTIONS antes).
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            self._send(200, DASHBOARD_HTML.encode("utf-8"), "text/html; charset=utf-8")
        elif self.path == "/api/status":
            try:
                payload = json.dumps(get_dashboard_status()).encode("utf-8")
                self._send(200, payload, "application/json")
            except Exception as exc:
                payload = json.dumps({"ready": False, "error": str(exc)}).encode("utf-8")
                self._send(500, payload, "application/json")
        elif self.path == "/api/devices":
            payload = json.dumps(get_device_state_payload()).encode("utf-8")
            self._send(200, payload, "application/json")
        elif self.path == "/api/cargas":
            try:
                # passive=True (misma caché que /api/status) — sin esto, cada
                # pedido forzaba una consulta activa al EcoFlow (lenta, unos
                # segundos), que era el retraso que se veía en el dashboard/app
                # antes de que apareciera la sección de gestión de cargas.
                message = build_load_advisor_message(_gather_metrics(passive=True)) if ECOFLOW_READY else ""
                payload = json.dumps({"message": message}).encode("utf-8")
                self._send(200, payload, "application/json")
            except Exception as exc:
                payload = json.dumps({"message": "", "error": str(exc)}).encode("utf-8")
                self._send(500, payload, "application/json")
        else:
            self._send(404, b"not found", "text/plain")

    def do_POST(self):
        if self.path == "/api/devices":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length) or b"{}")
                device = str(body.get("device", ""))
                # el panel web manda la clave exacta (dev.key de /api/devices),
                # no hace falta resolver alias como en los comandos de Telegram
                if device not in DEVICE_INFO:
                    self._send(400, b'{"error":"dispositivo desconocido"}', "application/json")
                    return
                DEVICE_STATE[device] = bool(body.get("on"))
                _save_persisted_state()
                payload = json.dumps(get_device_state_payload()).encode("utf-8")
                self._send(200, payload, "application/json")
            except Exception as exc:
                payload = json.dumps({"error": str(exc)}).encode("utf-8")
                self._send(500, payload, "application/json")
        elif self.path == "/api/alerta":
            global BATTERY_LOW_THRESHOLD
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length) or b"{}")
                pct = int(body.get("threshold_pct"))
                if not (0 <= pct <= 100):
                    self._send(400, b'{"error":"debe ser un porcentaje entre 0 y 100"}', "application/json")
                    return
                BATTERY_LOW_THRESHOLD = pct
                _save_persisted_state()
                payload = json.dumps({"threshold_pct": BATTERY_LOW_THRESHOLD}).encode("utf-8")
                self._send(200, payload, "application/json")
            except Exception as exc:
                payload = json.dumps({"error": str(exc)}).encode("utf-8")
                self._send(500, payload, "application/json")
        else:
            self._send(404, b"not found", "text/plain")


def run_dashboard_server() -> None:
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), _DashboardHandler)
    log.info("Dashboard web escuchando en el puerto %d", PORT)
    server.serve_forever()


def main() -> None:
    try:
        set_bot_commands()
    except Exception:
        log.exception("No se pudo registrar el menú de comandos (no bloqueante)")
    try:
        set_dashboard_menu_button()
    except Exception:
        log.exception("No se pudo configurar el botón del dashboard (no bloqueante)")

    threading.Thread(target=poll_commands, daemon=True).start()
    threading.Thread(target=report_timer, daemon=True).start()
    threading.Thread(target=battery_projection_timer, daemon=True).start()
    threading.Thread(target=ac_check_timer, daemon=True).start()
    threading.Thread(target=watchdog_timer, daemon=True).start()
    threading.Thread(target=quiet_hours_timer, daemon=True).start()
    threading.Thread(target=weekly_cleanup_timer, daemon=True).start()
    threading.Thread(target=run_dashboard_server, daemon=True).start()
    if USE_PRIVATE_API:
        threading.Thread(target=start_private_mqtt, daemon=True).start()

    log.info(
        "Monitor iniciado. Informe a las :00/:30 (pausado %02d:%02d-%02d:%02d), chequeo AC cada %.1f min.",
        QUIET_START_HOUR,
        QUIET_START_MINUTE,
        QUIET_END_HOUR,
        QUIET_END_MINUTE,
        AC_CHECK_MINUTES,
    )
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
