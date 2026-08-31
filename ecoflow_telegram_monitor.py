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
    "ecoplay": {"label": "Ecoplay", "emoji": "📡", "watts": 120},
    "tv": {"label": "TV", "emoji": "📺", "watts": 100},
}
for _base, (_count, _label, _emoji, _watts) in MULTI_UNIT_DEVICES.items():
    for _i in range(1, _count + 1):
        DEVICE_INFO[f"{_base}{_i}"] = {"label": f"{_label} {_i}", "emoji": _emoji, "watts": _watts}
DEVICE_INFO["ventilador3"]["watts"] = 10  # el tercer ventilador es más chico que los otros dos (20W)


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
                    "device_charged": DEVICE_CHARGED,
                    "ecoplay_pct": ECOPLAY_LAST_PCT,
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
_saved_device_charged = _persisted.get("device_charged", {})
DEVICE_CHARGED = {
    f"{b}{i}": bool(_saved_device_charged.get(f"{b}{i}", False))
    for b, (c, *_r) in MULTI_UNIT_DEVICES.items()
    for i in range(1, c + 1)
}
# Ecoplay entra a /cargado-/descargado como señal manual rápida, informativa
# nomás — coexiste con /ecoplay <pct> (el sistema más preciso que ya existe
# para ella) sin reemplazarlo. A diferencia de ventilador/powerbank, Ecoplay
# es de una sola unidad, así que NO participa del sorteo de prioridad de
# _multi_unit_line (esa función solo recibe listas de ventilador*/powerbank*
# — no hay nada que reordenar en un grupo de uno).
DEVICE_CHARGED["ecoplay"] = bool(_saved_device_charged.get("ecoplay", False))
ECOPLAY_LAST_PCT = _persisted.get("ecoplay_pct")
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
        {"command": "ecoplay", "description": "Hasta qué hora aguanta la batería propia de la Ecoplay (ej: /ecoplay 86)"},
        {"command": "help", "description": "Ver comandos disponibles"},
    ]
    resp = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands", json={"commands": commands}, timeout=30
    )
    resp.raise_for_status()


DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "").strip()
# Telegram cachea el Mini App por URL exacta del lado del cliente — el
# Cache-Control: no-store del propio server no alcanza, porque a veces ni
# siquiera vuelve a pedir el documento. Se le pega un query param que cambia
# en cada arranque del proceso (o sea, en cada deploy), así Telegram ve una
# URL nueva y se ve obligado a pedir el documento de nuevo en vez de reusar
# la versión vieja que tenía en su WebView.
_DASHBOARD_CACHE_BUST = str(int(time.time()))


def set_dashboard_menu_button() -> None:
    """Pone el botón del menú (al lado del clip, abajo a la izquierda) para
    que abra el dashboard como Web App adentro de Telegram, sin salir a un
    navegador aparte."""
    if not DASHBOARD_URL:
        return
    sep = "&" if "?" in DASHBOARD_URL else "?"
    url = f"{DASHBOARD_URL}{sep}v={_DASHBOARD_CACHE_BUST}"
    resp = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/setChatMenuButton",
        json={"menu_button": {"type": "web_app", "text": "📊 Panel", "web_app": {"url": url}}},
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


def _battery_remain(soc, net_w) -> dict | None:
    """Tiempo estimado por batería individual (Delta 2 o Extra por
    separado, no el combinado): si está cargando, cuánto falta para
    llegar al 100%; si está descargando, cuánto le queda de autonomía.
    Misma estimación lineal que el resto del bot (watts constantes)."""
    if soc is None or net_w is None or abs(net_w) <= NOISE_FLOOR_W:
        return None
    charging = net_w > 0
    target = 100 if charging else 0
    hours = abs(BATTERY_CAPACITY_WH * (target - soc) / 100 / net_w)
    h, m = divmod(int(round(hours * 60)), 60)
    return {"charging": charging, "text": f"{h}h {m}m"}


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


# Batería propia de la Ecoplay/WiFi (power bank aparte, no reporta telemetría
# como la Delta 2 — el usuario informa el % a mano con /ecoplay). Capacidad
# confirmada por el usuario: ~484 Wh. El consumo real medido es 35-45 W;
# se usa el techo (45 W, peor caso) para el cálculo de la hora segura, ya
# que es el que menos autonomía da y por lo tanto el que garantiza llegar
# a la meta incluso si el consumo real termina siendo el más alto.
ECOPLAY_BATTERY_WH = 484
ECOPLAY_MAX_W = 45
ECOPLAY_TARGET_HOUR = 7
ECOPLAY_TARGET_MINUTE = 30


def _next_ecoplay_target(now) -> datetime:
    target = now.replace(hour=ECOPLAY_TARGET_HOUR, minute=ECOPLAY_TARGET_MINUTE, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target


def _ecoplay_autonomy(pct: int, now=None) -> dict:
    """Dado el % que el usuario reporta a mano de la batería propia de la
    Ecoplay, calcula a qué hora, como mucho, conviene pasarla a esa batería
    para que llegue sin cortarse hasta el próximo objetivo (7:30 AM). Usa
    siempre el peor caso de consumo (45 W): da menos horas de autonomía que
    el resto del rango, así que arrancar recién ahí es lo que garantiza
    llegar a la meta incluso si el consumo real termina siendo el más alto."""
    now = now or datetime.now(TZ)
    wh_available = ECOPLAY_BATTERY_WH * pct / 100
    worst_hours = wh_available / ECOPLAY_MAX_W
    target = _next_ecoplay_target(now)
    safe_switch = target - timedelta(hours=worst_hours)
    # BUG (fixed): target siempre está en el futuro respecto de now (rueda
    # al próximo 07:30 por definición de _next_ecoplay_target), así que
    # comparar safe_switch > now es casi siempre True sin importar pct —
    # con worst_hours=0 (pct=0), safe_switch == target, que sigue siendo
    # > now. Eso hacía que has_autonomy diera True incluso con 0% de
    # batería. La autonomía real depende de worst_hours (cuántas horas
    # aguanta la batería propia al peor consumo), no de una comparación de
    # timestamps contra target. Epsilon de 0.05h (~3min) evita falsos
    # positivos por redondeo de punto flotante cuando pct es efectivamente 0.
    has_autonomy = worst_hours > 0.05
    return {
        "pct": pct,
        "wh_available": round(wh_available),
        "target_text": target.strftime("%H:%M"),
        "safe_switch_text": safe_switch.strftime("%H:%M"),
        "has_autonomy": has_autonomy,
    }


def _format_ecoplay_message(info: dict) -> str:
    if not info["has_autonomy"]:
        return (
            f"📡 Ecoplay al {info['pct']}%: no tiene autonomía como para pasarla a su batería propia "
            f"ahora mismo y aguantar hasta las {info['target_text']} — no la cambies todavía."
        )
    return (
        f"📡 Ecoplay al {info['pct']}%: podés poner la wifi en su batería propia a partir de "
        f"las ~{info['safe_switch_text']} para que aguante hasta las {info['target_text']}."
    )


def _ecoplay_cargas_suffix(now=None) -> str:
    """Nota corta para pegar a la línea de Ecoplay en Gestión de cargas —
    mismo dato que /ecoplay, pero solo si el usuario ya informó un %."""
    if ECOPLAY_LAST_PCT is None:
        return ""
    info = _ecoplay_autonomy(ECOPLAY_LAST_PCT, now)
    if not info["has_autonomy"]:
        return f" · 🔋 {info['pct']}%: sin autonomía todavía"
    return f" · 🔋 {info['pct']}%: {info['safe_switch_text']} (Meta: {info['target_text']})"


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

    def _nf(v):
        return int(v) if v is not None and v > NOISE_FLOOR_W else 0

    soc_delta2 = _pick(data, "bms_bmsStatus.f32ShowSoc", "bms_bmsStatus.soc", "pd.soc", "bmsMaster.soc")
    soc_extra = get_extra_battery_soc(data)
    extra_in_w = _pick(data, "bms_slave.inputWatts")
    extra_out_w = _pick(data, "bms_slave.outputWatts")
    pv_w = get_pv_watts(data)
    out_w = _pick(data, "pd.wattsOutSum", "inv.outputWatts", default=0)
    ac_out_w_raw = _pick(data, "inv.outputWatts")
    total_in_w = _pick(data, "pd.wattsInSum", default=(pv_w or 0))
    remain_min = _pick(data, "pd.remainTime", "bms_emsStatus.dsgRemainTime")

    # bms_bmsStatus.inputWatts/outputWatts (campo directo de la Delta 2) está
    # confirmado roto (pegado en 0); se deriva el neto de la contabilidad
    # general del sistema en su lugar. Para la batería extra, en cambio,
    # bms_slave.inputWatts/outputWatts SÍ son confiables — verificado en vivo
    # contra logs de producción (carga: inputWatts>0/outputWatts=0; descarga:
    # inputWatts=0/outputWatts>0, nunca los dos activos a la vez).
    delta2_net_w = total_in_w - out_w
    extra_net_w = (extra_in_w or 0) - (extra_out_w or 0) if extra_in_w is not None else None
    system_net_w = delta2_net_w + (extra_net_w or 0)
    extra_in_w_nf = _nf(extra_in_w)
    # extra_out_w: descarga bruta de la batería extra (bms_slave.outputWatts),
    # separada de extra_in_w (carga) — alimenta el nodo "Extra" de arriba, que
    # ahora SIEMPRE muestra descarga (no neto). Ver sdd/power-flow-bottom-nodes.
    extra_out_w_nf = _nf(extra_out_w)
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
    active_ports = [{"name": name, "watts": w} for name, w in ports if w and w > NOISE_FLOOR_W]

    ac_out_w = _nf(ac_out_w_raw)
    usb_out_w = _nf(
        (_pick(data, "pd.typec1Watts") or 0)
        + (_pick(data, "pd.typec2Watts") or 0)
        + (_pick(data, "pd.usb1Watts") or 0)
        + (_pick(data, "pd.usb2Watts") or 0)
    )

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
        "ac_out_w": ac_out_w,
        "extra_in_w": extra_in_w_nf,
        "extra_out_w": extra_out_w_nf,
        "usb_out_w": usb_out_w,
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
    "/on <dispositivo> — marcarlo encendido (nevera, laptop, ecoplay, tv, ventilador, powerbank)\n"
    "/off <dispositivo> — marcarlo apagado\n"
    "/cargado <ventilador/powerbank/ecoplay> — marcar como cargada; en ventilador/powerbank "
    "además prioriza el resto en el próximo reparto de excedente (ej: /cargado ventilador1 ventilador2). "
    "En ecoplay es solo informativo (no afecta prioridades) — para el dato preciso seguí usando /ecoplay <pct>\n"
    "/descargado <ventilador/powerbank/ecoplay> — marcarla como descargada (en ventilador/powerbank, "
    "prioridad para recibir carga)\n"
    "/alerta <porcentaje> — avisar cuando la carga baje de ese nivel (ej: /alerta 20)\n"
    "/ecoplay <porcentaje> — hasta qué hora aguanta la batería propia de la Ecoplay/WiFi "
    "(35-45 W) para llegar a las 7:30 AM (ej: /ecoplay 86)\n"
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
    global BATTERY_LOW_THRESHOLD, ECOPLAY_LAST_PCT
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
    elif cmd in ("/cargado", "/descargado"):
        if len(parts) < 2:
            send_telegram(f"Uso: {cmd} <dispositivo> — {', '.join(DEVICE_CHARGED)}", chat_id=chat_id)
        else:
            resolved, invalid = [], []
            for w in parts[1:]:
                valid = [k for k in _resolve_device_keys(w) if k in DEVICE_CHARGED]
                (resolved.extend(valid) if valid else invalid.append(w))
            em = "🔋" if cmd == "/cargado" else "🪫"
            estado = "cargada" if cmd == "/cargado" else "descargada"
            lines = []
            if resolved:
                for k in resolved:
                    DEVICE_CHARGED[k] = cmd == "/cargado"
                # Ecoplay es la única con sistema de % propio (/ecoplay <pct>);
                # al marcarla descargada por acá, sincronizamos ese % a 0 para
                # que /cargas y _ecoplay_cargas_suffix reflejen lo mismo que
                # el flag binario. Ventilador/powerbank no tienen % análogo.
                if cmd == "/descargado" and "ecoplay" in resolved:
                    ECOPLAY_LAST_PCT = 0
                _save_persisted_state()
                lines.extend(f"{em} {DEVICE_INFO[k]['label']}: {estado}" for k in resolved)
            if invalid:
                lines.append(
                    f"No reconozco / no es multi-unidad: {', '.join(invalid)}. Solo: {', '.join(DEVICE_CHARGED)}"
                )
            send_telegram("\n".join(lines), chat_id=chat_id)
    elif cmd == "/ecoplay":
        if len(parts) < 2 or not parts[1].isdigit() or not (0 <= int(parts[1]) <= 100):
            send_telegram("Uso: /ecoplay <porcentaje entre 0 y 100>, ej: /ecoplay 86", chat_id=chat_id)
        else:
            ECOPLAY_LAST_PCT = int(parts[1])
            _save_persisted_state()
            info = _ecoplay_autonomy(ECOPLAY_LAST_PCT)
            send_telegram(_format_ecoplay_message(info), chat_id=chat_id)
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
# 19:30, que dice qué debería estar encendido/apagado según el excedente real
# del sistema (que cubre las 24 h: la noche solo se consulta por /cargas, el
# timer automático no manda mensajes fuera de esa ventana). El frío
# (congelador) se eliminó del sistema — la NEVERA tomó su rol: es la carga
# protegida, se mantiene ON siempre salvo emergencia de batería, y se apaga
# programado a las 12 AM (aguanta cerrada hasta el amanecer). Laptop,
# Ventilador, Power bank y TV se reparten el excedente real en orden de
# prioridad (ver build_load_advisor_message) — ninguna tiene ventana horaria
# fija, todas se evalúan contra system_net_w en el momento de la consulta.
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
     "battery_goal": "Amanecer con 15%+"},
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
    priority = sorted(device_keys, key=lambda k: DEVICE_CHARGED.get(k, False))
    dot_by_key = {}
    for key in priority:
        watts = DEVICE_INFO[key]["watts"]
        if watts <= remaining:
            dot_by_key[key] = "🟢"
            remaining -= watts
        else:
            dot_by_key[key] = "🔴"
    dots = [dot_by_key[key] for key in device_keys]  # render in original order
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

_BATTERY_EMERGENCY_ACTIVE = False  # trackea la transición para loguear una sola vez al entrar/salir, no en cada llamada


def build_load_advisor_message(m: dict = None) -> str:
    """Nevera, Laptop, TV, Power bank, Ventilador y Ecoplay — cada una ya
    evalúa el estado real (watts, batería) en vez de ser un texto fijo.
    Prioridad: Nevera (protegida, no compite por excedente) > Laptop >
    Ventilador > Power bank > Ecoplay > TV — estas últimas cinco se reparten
    el MISMO excedente (system_net_w) en orden, restando lo que cada una se
    lleva antes de evaluar la siguiente. Ecoplay va antes que TV porque ahora
    consume de la Delta 2 como cualquier otra carga y el usuario pidió
    mantenerla simple, sin trato especial. Antes cada una miraba el excedente
    total por separado, lo que podía mostrar varias en verde a la vez aunque
    juntas no entraran. Por debajo de BATTERY_EMERGENCY_THRESHOLD se apaga
    TODO, Ecoplay incluida (sin excepción de 'es internet, dejalo prendido')
    — no alcanza con bajar solo la nevera si el resto sigue mostrando 'ON'
    como si nada, porque son de menor prioridad y deben ceder primero/junto
    con ella."""
    block = _current_load_block()
    if block is None:
        return ""
    now = datetime.now(TZ)
    if m is None:
        m = _gather_metrics()
    avg_soc_str = f"{m['avg_soc']:.1f}%" if m["avg_soc"] is not None else "N/D"
    emergency = block["nevera"] != "off_midnight" and m["avg_soc"] is not None and m["avg_soc"] < BATTERY_EMERGENCY_THRESHOLD

    global _BATTERY_EMERGENCY_ACTIVE
    if emergency and not _BATTERY_EMERGENCY_ACTIVE:
        log.warning("Emergencia de batería iniciada: %.1f%% (umbral %d%%)", m["avg_soc"], BATTERY_EMERGENCY_THRESHOLD)
        _BATTERY_EMERGENCY_ACTIVE = True
    elif not emergency and _BATTERY_EMERGENCY_ACTIVE:
        log.info("Emergencia de batería terminada: %.1f%%", m["avg_soc"])
        _BATTERY_EMERGENCY_ACTIVE = False

    if emergency:
        vent_line, _ = _multi_unit_line("🌀", "Ventilador", VENTILADOR_DEVICE_KEYS, 0)
        pb_line, _ = _multi_unit_line("🔋", "Power bank", POWERBANK_DEVICE_KEYS, 0)
        lines = [
            f"🔆 *Gestión de cargas* · {block['label']}",
            f"🚨 EMERGENCIA DE BATERÍA — {avg_soc_str}, apagar todo",
            _status_line("🥶", "Nevera", False, ["nevera"]),
            _status_line("💻", "Laptop", False, ["laptop"]),
            vent_line,
            pb_line,
            _status_line("📡", "Ecoplay", False, ["ecoplay"]) + _ecoplay_cargas_suffix(now),
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

        # Ecoplay ahora consume de la Delta 2 como cualquier otra carga —
        # va antes que TV en la cola porque el usuario pidió simplicidad, sin
        # trato especial por ser "internet" (antes tenía alimentación
        # propia y por eso quedaba fija en verde).
        ecoplay_ok, ecoplay_detail, available = _allocate_budget(DEVICE_INFO["ecoplay"]["watts"], available)
        if ecoplay_ok or not DEVICE_STATE.get("ecoplay"):
            ecoplay_detail = ""
        ecoplay_line = _status_line("📡", "Ecoplay", ecoplay_ok, ["ecoplay"], ecoplay_detail) + _ecoplay_cargas_suffix(now)

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
            ecoplay_line,
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
# (15% al amanecer, 20% a las 9 AM, ~55-60% al mediodía, 65-75% a las 3 PM,
# 100% al cierre del día — este último solo es realista con nevera+internet
# nomás). Los
# checkpoints son cíclicos: si ya pasaron todos los de hoy,
# se proyecta contra el primero de mañana (el amanecer), cruzando la
# medianoche — así de 8 PM en adelante el mensaje evalúa si vas a sobrevivir
# la noche, no se queda mudo hasta el mediodía siguiente. Avisa ANTES de que
# pase, no cuando ya se descontroló. Chequea más seguido que el mensaje de
# 30 min para reaccionar rápido a caídas bruscas.
PROJECTION_CHECK_MINUTES = 10
PROJECTION_ALERT_MARGIN = 5  # puntos porcentuales por debajo de la meta para disparar la alerta
BATTERY_CHECKPOINTS = [
    (6 * 60, 15, "el amanecer"),
    (9 * 60, 20, "las 9:00 AM"),
    (12 * 60, 55, "el mediodía"),
    (15 * 60, 65, "las 3:00 PM"),
    (19 * 60 + 30, 100, "el cierre del día (7:30 PM)"),
]  # ordenados por hora del día — el orden importa para _next_checkpoint. El
# checkpoint de las 9 AM coincide con el cierre del bloque de gestión de
# cargas de esa franja — así la proyección de /cargas siempre habla del
# mismo horizonte que el período que se está mostrando, en vez de saltar
# directo al mediodía y generar confusión (ej. "estamos en el período de
# hasta las 9 pero dice que para el mediodía"). El
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


_checkpoint_logged_for = {}  # {cp_min: date} último día que ya se dejó constancia de ese checkpoint


def _log_checkpoint_result(now, avg_soc) -> None:
    """Deja constancia en los logs de Railway (no manda nada a Telegram) de
    si cada checkpoint del plan se cumplió o no apenas pasa su horario, y
    por cuántos puntos — para tener un registro histórico sin depender de
    revisar los mensajes uno por uno. Se ejecuta en la misma cadencia que
    el chequeo de proyección (cada PROJECTION_CHECK_MINUTES), así que basta
    con loguear una vez por checkpoint/día apenas su hora cae dentro de esa
    ventana."""
    if avg_soc is None:
        return
    minute_of_day = now.hour * 60 + now.minute
    today = now.date()
    for cp_min, floor, label in BATTERY_CHECKPOINTS:
        if cp_min <= minute_of_day < cp_min + PROJECTION_CHECK_MINUTES and _checkpoint_logged_for.get(cp_min) != today:
            diff = avg_soc - floor
            if diff >= 0:
                log.info("Checkpoint '%s' CUMPLIDO: %.1f%% (meta %d%%+, sobró %.1f pts)", label, avg_soc, floor, diff)
            else:
                log.info("Checkpoint '%s' NO CUMPLIDO: %.1f%% (meta %d%%+, faltaron %.1f pts)", label, avg_soc, floor, -diff)
            _checkpoint_logged_for[cp_min] = today


def _check_battery_projection(now=None) -> None:
    """Un chequeo: proyecta el %SOC en el próximo checkpoint con el ritmo de
    descarga actual y avisa (una vez por checkpoint/día, con rearme si se
    recupera) si va a quedar por debajo de la meta menos el margen."""
    now = now or datetime.now(TZ)
    minute_of_day = now.hour * 60 + now.minute
    if not (LOAD_ADVISOR_START_MIN <= minute_of_day < LOAD_ADVISOR_END_MIN):
        return
    m = _gather_metrics()
    _log_checkpoint_result(now, m["avg_soc"])
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
            # "Mejor caso": cuánto cambiaría la proyección si apagaras TODA la
            # carga discrecional marcada (laptop, TV, ventilador, power bank —
            # nevera/internet quedan afuera porque son protegidas). Si ni así
            # se llega a la meta, decir "bajá carga" es engañoso: el problema
            # no es cuánto estás gastando, es que no hay sol suficiente.
            discretionary_keys = ["laptop", "tv"] + VENTILADOR_DEVICE_KEYS + POWERBANK_DEVICE_KEYS
            freeable_w = sum(DEVICE_INFO[k]["watts"] for k in discretionary_keys if DEVICE_STATE.get(k))
            best_case_net_w = m["system_net_w"] + freeable_w
            best_proj = _project_to_checkpoint(now, m["avg_soc"], best_case_net_w)
            best_case_helps = best_proj is not None and best_proj[2] >= floor - PROJECTION_ALERT_MARGIN

            if best_case_helps:
                action = "Bajá carga (laptop, TV, power bank, ventilador) — la nevera es protegida, no la toques."
            else:
                action = (
                    "Ni apagando todo lo que se puede llegás a esa meta ahora mismo — "
                    "no es problema de consumo, no hay sol suficiente en lo que queda."
                )
            send_telegram(
                "⚠️ *Se está yendo de control*\n\n"
                f"Proyectás *{projected:.0f}%* para {label} (meta {floor}%+)\n"
                f"Vas en {m['avg_soc']:.1f}%, descargando a {round(m['system_net_w'])} W.\n\n"
                f"{action}"
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
            {
                "key": key,
                "label": info["label"],
                "emoji": info["emoji"],
                "watts": info["watts"],
                "on": DEVICE_STATE[key],
                "charged": DEVICE_CHARGED.get(key),
            }
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
    delta2_remain = _battery_remain(m["soc_delta2"], m["delta2_net_w"])
    extra_remain = _battery_remain(m["soc_extra"], m["extra_net_w"])

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

    # Staleness real de la telemetría MQTT: mismo criterio que watchdog_timer
    # (línea ~1112), no solo "el fetch HTTP al propio servidor respondió".
    # Sin esto el dashboard mostraba "conectado" con MQTT caído hace horas,
    # porque el HTTP local seguía respondiendo con el último cache guardado.
    with _mqtt_cache_lock:
        entry = _device_cache.get(SN_DELTA2, {})
        updated_at = entry.get("updated_at", 0)
    stale_minutes = round((datetime.now(TZ).timestamp() - updated_at) / 60, 1) if updated_at else None
    is_stale = stale_minutes is None or stale_minutes > WATCHDOG_STALE_MINUTES

    now = datetime.now(TZ)
    goal_label = goal_floor = goal_projected = goal_met = None
    proj = _project_to_checkpoint(now, m["avg_soc"], m["system_net_w"])
    if proj:
        goal_label, goal_floor, goal_projected = proj
        goal_met = goal_projected >= goal_floor

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
        "delta2_remain": delta2_remain,
        "extra_remain": extra_remain,
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
        "ac_out_w": m["ac_out_w"] or 0,
        "extra_in_w": m["extra_in_w"] or 0,
        "extra_out_w": m["extra_out_w"] or 0,
        "usb_out_w": m["usb_out_w"] or 0,
        "updated_at": datetime.now(TZ).strftime("%H:%M:%S"),
        "stale": is_stale,
        "stale_minutes": stale_minutes,
        "goal_label": goal_label,
        "goal_floor": goal_floor,
        "goal_projected": goal_projected,
        "goal_met": goal_met,
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
    display: flex; flex-direction: column; align-items: center; padding: 20px 16px 32px;
  }
  .io-row {
    width: 100%; max-width: 380px; display: flex; justify-content: space-between;
    align-items: flex-start; margin-bottom: 12px;
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
  /* RING SIZE: restored to the original 240px (was shrunk to 190px in an
     earlier batch to make horizontal room for the lateral battery node,
     back when that node lived in a shared flex row beside the ring). Now
     that the lateral node is an absolutely-positioned overlay (see
     .lateral-overlay below) that consumes ZERO layout width, the ring no
     longer needs to give anything up — restored to 240px, and
     .pct/.pct-sub/.dur font sizes below are scaled back up proportionally
     (reversing the earlier x0.79 shrink) to match. */
  .ring-wrap { position: relative; width: 240px; height: 240px; flex-shrink: 0; margin: 6px 0 8px; }
  /* The ring is centered ON ITS OWN via the body's flex column
     (align-items:center) — same simple centering the AC/Solar and CA/USB
     rows rely on. It is NOT wrapped in a shared flex row with the lateral
     battery node anymore (that composite-centering approach made the ring
     look off-center relative to the rows above/below it, since the extra
     width lived only on one side). The battery node instead overlays out
     via .lateral-overlay, anchored to .ring-wrap's own box (position:
     relative) but positioned absolute so it doesn't affect centering. */
  .flow-top-wrap { position: relative; width: 300px; max-width: 100%; margin: 0 auto 4px; padding-bottom: 122px; }
  .flow-bottom-wrap { position: relative; width: 300px; max-width: 100%; margin: 4px auto 0; padding-top: 122px; }
  .flow-connectors { position: absolute; left: 0; width: 100%; height: 130px; pointer-events: none; top: 0; }
  .flow-connectors.top { top: auto; bottom: 0; }
  .flow-overlay {
    fill: none; stroke: #4ade80; stroke-width: 2; stroke-linecap: round;
    stroke-dasharray: 6 10; opacity: 0; transition: opacity 200ms var(--ease-out);
  }
  #flow-ac-out.flow-overlay, #flow-usb-out.flow-overlay, #flow-lateral-discharge.flow-overlay { stroke: #f87171; }
  .flow-overlay.active { opacity: 1; animation: flow-dash 1.1s linear infinite; }
  @keyframes flow-dash { to { stroke-dashoffset: -16; } }
  /* width:300px + max-width:100% (NOT a fixed 300px) so these rows shrink
     on real narrow viewports instead of silently overflowing past the
     visible edge — same responsive pattern already used by
     .flow-top-wrap/.flow-bottom-wrap above. */
  .icons-row.top { width: 300px; max-width: 100%; margin: 0 auto; margin-bottom: 0; }
  .icons-row.bottom { width: 300px; max-width: 100%; margin: 0 auto; margin-bottom: 0; }
  /* .lateral-overlay anchors at the ring's own right edge, vertical
     midpoint (left:240px = ring width, top:120px = half of ring height).
     width/height:0 so it consumes NO layout space of its own — it's a
     pure positioning origin for its two absolutely/statically-flowed
     children (the hook SVG + the battery icon-item below it). See the
     full GEOMETRY SPEC + fit-check arithmetic in the HTML comment above
     .ring-wrap below. */
  .lateral-overlay { position: absolute; left: 240px; top: 120px; width: 0; height: 0; }
  .lateral-overlay .icon-item.lateral-icon { position: absolute; left: 28px; top: 90px; }
  .flow-connectors.lateral { width: 58px; height: 96px; overflow: visible; display: block; }
  .flow-connectors.lateral .flow-overlay {
    stroke-dasharray: 3 4; stroke-linecap: butt; stroke-width: 2.5;
    animation-name: flow-dash-lateral !important;
    animation-duration: 0.96s !important;
  }
  #flow-lateral-charge, #flow-lateral-discharge { stroke-width: 2.5; }
  @keyframes flow-dash-lateral { to { stroke-dashoffset: -14; } }
  /* Compact variant of .icon-item, used ONLY by the lateral battery node.
     Narrower (56px vs 84px) with a smaller circle/text so the overlay's
     own icon footprint stays small — see fit-check arithmetic in the
     GEOMETRY SPEC comment above .ring-wrap in the HTML below. */
  .icon-item.lateral-icon { width: 56px; }
  .icon-item.lateral-icon .icon-circle { width: 46px; height: 46px; font-size: 19px; }
  .icon-item.lateral-icon .icon-watts { font-size: 11px; margin-top: 3px; }
  .icon-item.lateral-icon .icon-name { font-size: 9px; }
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
  .pct-sub { font-size: 14px; color: #9aa4af; margin-top: 6px; text-align: center; }
  .pct-sub .dur { font-size: 23px; color: #e5e7eb; font-weight: 700; margin-top: 2px; font-variant-numeric: tabular-nums; }
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
  .eta-box .eta-goal {
    font-size: 13px; font-weight: 700; margin-top: 8px; padding-top: 8px;
    border-top: 1px solid #232c36; font-variant-numeric: tabular-nums;
  }
  .eta-goal.eta-ok { color: #4ade80; }
  .eta-goal.eta-warn { color: #f87171; }
  .batteries { width: 100%; max-width: 380px; margin-top: 10px; }
  .battery-row {
    display: flex; justify-content: space-between; align-items: center;
    background: #141b22; border-radius: 14px; padding: 12px 16px; margin-top: 8px;
  }
  .battery-row .name { font-size: 14px; color: #cbd5e1; display: flex; align-items: center; gap: 6px; }
  .battery-row .sub { font-size: 12px; color: #6b7684; font-weight: 400; }
  .battery-row .val { font-size: 16px; font-weight: 700; font-variant-numeric: tabular-nums; }
  .battery-row .val.charging { color: #4ade80; }
  .battery-row .val.discharging { color: #f87171; }
  .usb-svg { flex-shrink: 0; color: #9aa4af; }
  .devices { width: 100%; max-width: 380px; margin-top: 10px; }
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
  .cargas { width: 100%; max-width: 380px; margin-top: 10px; }
  .cargas .title { font-size: 13px; color: #9aa4af; margin-bottom: 6px; }
  .cargas-box {
    font-size: 14px; line-height: 1.7; color: #cbd5e1; white-space: pre-line;
    background: #141b22; border: 1px solid #232b33; border-radius: 10px; padding: 12px 14px;
  }
  .modal-backdrop {
    display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.6);
    align-items: center; justify-content: center; padding: 16px; z-index: 100;
  }
  .modal-backdrop.visible { display: flex; }
  .modal-box {
    background: #141b22; border: 1px solid #232b33; border-radius: 16px;
    padding: 18px 20px; width: 100%; max-width: 320px;
  }
  .modal-title { font-size: 15px; font-weight: 700; color: #f5f5f5; margin-bottom: 12px; }
  .modal-input {
    width: 100%; font-size: 16px; padding: 10px 12px; border-radius: 10px;
    background: #0b0f14; border: 1px solid #232b33; color: #f5f5f5;
    font-variant-numeric: tabular-nums; margin-bottom: 12px;
  }
  .modal-actions { display: flex; gap: 8px; }
  .modal-btn {
    flex: 1; font-size: 14px; padding: 9px 12px; border-radius: 10px;
    background: #1c232b; border: 1px solid #232b33; color: #cbd5e1; cursor: pointer;
  }
  .modal-btn-primary { background: #14351f; border-color: #4ade8055; color: #4ade80; font-weight: 700; }
  .modal-result-ok, .modal-result-warn, .modal-result-error {
    margin-top: 12px; font-size: 13px; line-height: 1.5; padding: 10px 12px; border-radius: 10px;
  }
  .modal-result-ok { background: #14351f; color: #4ade80; }
  .modal-result-warn { background: #3a1616; color: #f87171; }
  .modal-result-error { background: #3a1616; color: #f87171; }
  .updated { margin-top: 14px; font-size: 12px; color: #7b8794; }
  .live-dot {
    display: inline-block; width: 6px; height: 6px; border-radius: 50%;
    background: #6b7684; margin-right: 6px; vertical-align: middle;
    transition: background-color 300ms var(--ease-out);
  }
  .live-dot.ok { background: #4ade80; }
  .live-dot.stale { background: #ef4444; animation: pulse 1s ease-in-out infinite; }
  #mqtt-stale-text { color: #f87171; }
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

  <!-- GEOMETRY SPEC (top, mirrored, manifold/elbow style): sdd/power-flow-bottom-nodes/design §4 —
       viewBox 0 0 300 130, hub (150,122) at the ring's top edge, nodes
       x=50/250 y=8 (bottom-center of each top node). AC and Solar are the
       two OUTER positions of the original 3-slot layout (x=50/150/250) —
       the middle slot (x=150, "Extra") was removed per
       sdd/power-flow-bottom-nodes+load-charge-priority consolidation
       (Extra/top + Batería/bottom merged into one lateral node next to the
       ring, see the GEOMETRY SPEC comment on .lateral-overlay below). AC/Solar
       paths are UNCHANGED — deleting the middle node doesn't require
       touching their geometry. Each side node drops straight down to a
       shared horizontal bus at y=65 (rounded 10px corners), then a single
       shared vertical trunk continues from the bus center (150,65) down to
       the hub. KEEP IN SYNC WITH App.tsx top connector Svg (and vice-versa). -->
  <div class="flow-top-wrap">
    <div class="icons-row top">
      <div class="icon-item">
        <div class="icon-circle" id="ac-circle">🔌</div>
        <div class="icon-watts" id="ac-w">0 W</div>
        <div class="icon-dir" id="ac-status"></div>
        <div class="icon-name">AC</div>
      </div>
      <div class="icon-item">
        <div class="icon-circle" id="pv-circle">☀️</div>
        <div class="icon-watts" id="pv-w">0 W</div>
        <div class="icon-name">Solar</div>
      </div>
    </div>
    <svg class="flow-connectors top" viewBox="0 0 300 130" preserveAspectRatio="none" width="300" height="130">
      <path d="M 75,8 L 75,55 Q 75,65 85,65 L 140,65 Q 150,65 150,75 L 150,122" fill="none" stroke="#232c36" stroke-width="2"/>
      <path id="flow-ac-top" class="flow-overlay" d="M 75,8 L 75,55 Q 75,65 85,65 L 140,65 Q 150,65 150,75 L 150,122"/>
      <path d="M 225,8 L 225,55 Q 225,65 215,65 L 160,65 Q 150,65 150,75 L 150,122" fill="none" stroke="#232c36" stroke-width="2"/>
      <path id="flow-solar-top" class="flow-overlay" d="M 225,8 L 225,55 Q 225,65 215,65 L 160,65 Q 150,65 150,75 L 150,122"/>
    </svg>
  </div>

  <!-- GEOMETRY SPEC (ring + lateral battery hook) — REWRITTEN this batch
       per explicit user rejection of the previous "ring-row" composite
       (ring + battery centered together as one flex unit). That composite
       made the ring look off-center relative to the AC/Solar and CA/USB
       rows above/below it, since those rows center independently while
       the ring+battery composite had a different effective center due to
       the extra width living only on the battery's side.

       FIX: the ring (.ring-wrap) is centered ON ITS OWN again, directly as
       a body flex-column child (align-items:center — same mechanism the
       old pre-composite design used, and equivalent in spirit to the
       margin:auto centering AC/Solar and CA/USB rows use). The battery
       node is now a small decorative overlay: a short stub pokes out of
       the ring's right edge, bends 90° via a rounded Q-corner (same
       elbow visual grammar as flow-ac-top/flow-solar-top/flow-ac-out/
       flow-usb-out), then drops straight down, with the battery icon
       hanging below the bend — positioned via .lateral-overlay
       (position:absolute, anchored to .ring-wrap which has
       position:relative) so it consumes ZERO horizontal layout space and
       cannot affect the ring's own centering; it just visually overflows
       outward, like a badge/annotation over a chart.

       Concrete geometry (ring restored to 240px this batch — was 190px
       under the old composite design, see .ring-wrap comment above in
       <style>): anchor point = ring's right edge, vertical midpoint
       (left:240px, top:120px, relative to .ring-wrap). Hook path (local
       coords from that anchor): stub 10px right -> "M 0,0 L 10,0", Q
       quarter-corner (radius 4) bending down -> "Q 14,0 14,4", vertical
       drop 50px -> "L 14,54". flow-lateral-discharge's `d` is the exact
       reverse point sequence so charge/discharge read in opposite visual
       directions (existing per-direction-path convention, see DIRECTION
       note above .flow-bottom-wrap). The battery icon-item (.lateral-icon,
       56px wide) sits left:-26px top:54px relative to the same anchor —
       i.e. mostly centered under the drop line, biased slightly left so
       its RIGHT edge (the only edge that matters for clipping, since it's
       the side facing the viewport edge) stays as close to the ring as
       possible. Vertically the icon ends around local y=120(anchor)+54+
       ~69(icon height)=~243, i.e. just below the ring's own 240px bottom
       edge, landing in the gap area before .flow-bottom-wrap's CA/USB row
       starts (which reserves 122px of top padding for its own connector
       SVG) — no vertical overlap.

       Fit-check arithmetic (content width = viewport width - 32px body
       padding, ring 240px centered via body flex, hook+icon rightmost
       extends 30px beyond the ring's own right edge: 14(hook offset to
       drop line) + 16(icon's right portion beyond the drop line, since
       icon left:-26/width:56 -> right edge is 30px past the anchor) —
       re-verify: icon right edge relative to anchor = -26+56 = 30, hook
       offset itself (14) is INSIDE that span, so the binding rightmost
       extent is exactly 30px past the ring's right edge):
         320px viewport -> 288px content -> ring left offset (288-240)/2 =
           24px -> ring abs right edge = 16(body pad)+24+240 = 280px ->
           hook+icon rightmost = 280+30 = 310px -> 320-310 = 10px spare
         360px viewport -> 328px content -> ring left offset 44px ->
           ring abs right edge = 16+44+240 = 300px -> rightmost = 330px ->
           360-330 = 30px spare
         375px viewport -> 343px content -> ring left offset 51.5px ->
           ring abs right edge = 16+51.5+240 = 307.5px -> rightmost =
           337.5px -> 375-337.5 = 37.5px spare
       All positive, including the narrowest realistic 320px target (10px
       spare) — no clipping. This is tighter than the old composite's
       margin (21px spare at 320px with a 190px ring) because the ring is
       now back to full 240px size, but it still fits safely.
       KEEP IN SYNC WITH App.tsx lateral connector Svg (and vice-versa). -->
  <div class="ring-wrap">
    <div class="ring" id="ring">
      <div class="ring-inner">
        <div class="pct" id="pct">--%</div>
        <div class="pct-sub">Tiempo restante<div class="dur" id="dur">--</div></div>
      </div>
    </div>
    <div class="lateral-overlay">
      <svg class="flow-connectors lateral" width="58" height="96" viewBox="0 0 58 96">
        <path d="M 0,0 L 48,0 Q 56,0 56,8 L 56,90" fill="none" stroke="#232c36" stroke-width="2"/>
        <path id="flow-lateral-charge" class="flow-overlay" d="M 0,0 L 48,0 Q 56,0 56,8 L 56,90"/>
        <path id="flow-lateral-discharge" class="flow-overlay" d="M 56,90 L 56,8 Q 56,0 48,0 L 0,0"/>
      </svg>
      <div class="icon-item lateral-icon">
        <div class="icon-circle" id="lateral-circle">🔋</div>
        <div class="icon-watts" id="lateral-w">0 W</div>
        <div class="icon-name">Batería</div>
      </div>
    </div>
  </div>

  <!-- GEOMETRY SPEC (manifold/elbow style): sdd/power-flow-bottom-nodes/design §4 —
       viewBox 0 0 300 130, hub (150,8), nodes x=50/150/250 y=122. Each side
       node connects to a shared horizontal bus at y=65 (rounded 10px
       corners), then a single shared vertical trunk continues from the
       bus center (150,65) to the hub. Center node is a straight vertical
       line (already aligned with hub x).
       DIRECTION: top-row paths (Entrada/input, see svg above) are defined
       node -> hub (source feeds the ring). Bottom-row paths below are
       defined hub -> node (the ring feeds the device) — opposite winding
       direction of the top row, same visual geometry, so the flow-dash
       animation reads correctly for each direction. Do not "normalize"
       bottom-row d start/end to match top row without re-checking this.
       CA and USB are the two OUTER positions of the original 3-slot layout
       (x=50/150/250) — the middle slot (x=150, "Batería") was removed, see
       the .lateral-overlay GEOMETRY SPEC above (Extra/top + Batería/bottom
       consolidated into one lateral node next to the ring). CA/USB paths
       are UNCHANGED. KEEP IN SYNC WITH App.tsx connector Svg (and
       vice-versa). -->
  <div class="flow-bottom-wrap">
    <svg class="flow-connectors" viewBox="0 0 300 130" preserveAspectRatio="none" width="300" height="130">
      <path d="M 150,8 L 150,55 Q 150,65 140,65 L 85,65 Q 75,65 75,75 L 75,122" fill="none" stroke="#232c36" stroke-width="2"/>
      <path id="flow-ac-out" class="flow-overlay" d="M 150,8 L 150,55 Q 150,65 140,65 L 85,65 Q 75,65 75,75 L 75,122"/>
      <path d="M 150,8 L 150,55 Q 150,65 160,65 L 215,65 Q 225,65 225,75 L 225,122" fill="none" stroke="#232c36" stroke-width="2"/>
      <path id="flow-usb-out" class="flow-overlay" d="M 150,8 L 150,55 Q 150,65 160,65 L 215,65 Q 225,65 225,75 L 225,122"/>
    </svg>
    <div class="icons-row bottom">
      <div class="icon-item">
        <div class="icon-circle">🔌</div>
        <div class="icon-watts" id="ac-out-w">0 W</div>
        <div class="icon-name">CA</div>
      </div>
      <div class="icon-item">
        <div class="icon-circle" id="usb-circle"><svg class="usb-svg" viewBox="0 0 24 12" width="18" height="9"><rect x="1" y="1" width="22" height="10" rx="5" fill="none" stroke="currentColor" stroke-width="2"/></svg></div>
        <div class="icon-watts" id="usb-out-w">0 W</div>
        <div class="icon-name">USB</div>
      </div>
    </div>
  </div>

  <div class="eta-box" id="eta-box">
    <div class="eta-main" id="eta-main"></div>
    <div class="eta-sub" id="eta-sub"></div>
    <div class="eta-goal" id="eta-goal"></div>
  </div>

  <div class="batteries" id="batteries"></div>

  <div class="cargas" id="cargas-wrap" style="display:none">
    <div class="title">Gestión de cargas</div>
    <div class="cargas-box" id="cargas-box"></div>
  </div>

  <!-- Modal simple para cargar el % de Ecoplay sin pasar por Telegram.
       Solo alcanzable tocando el badge de Ecoplay en "Estado de carga"
       cuando está "descargada" (ver renderCargaEstado) — el link
       standalone "Editar % Ecoplay" que existía antes fue removido por
       ser un trigger redundante. Reusa el estilo dark de .eta-box/
       .cargas-box (mismo bg #141b22, radios, colores de acento) en vez de
       inventar un lenguaje visual nuevo. DOM/JS vanilla, sin framework,
       igual que el resto del dashboard. -->
  <div class="modal-backdrop" id="ecoplay-modal-backdrop">
    <div class="modal-box">
      <div class="modal-title">Ecoplay: % de batería propia</div>
      <input type="number" id="ecoplay-pct-input" class="modal-input" min="0" max="100" placeholder="0-100">
      <div class="modal-actions">
        <button type="button" id="ecoplay-modal-submit" class="modal-btn modal-btn-primary">Aceptar</button>
        <button type="button" id="ecoplay-modal-close" class="modal-btn">Cerrar</button>
      </div>
      <div id="ecoplay-modal-result"></div>
    </div>
  </div>

  <div class="devices" id="carga-estado-wrap" style="display:none">
    <div class="title">Estado de carga</div>
    <div id="carga-estado"></div>
  </div>

  <div class="devices">
    <div class="title">Qué tenés encendido</div>
    <div id="devices"></div>
  </div>

  <div class="updated">
    <span class="live-dot" id="live-dot"></span>
    <span id="updated-text"></span>
    <span id="mqtt-stale-text"></span>
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
        const etaGoal = document.getElementById('eta-goal');
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
        } else if (!d.goal_label) {
          etaBox.classList.remove('visible');
        }

        // Meta/checkpoint del próximo horario del plan — misma proyección
        // que ya usa /cargas (_project_to_checkpoint), integrada dentro de
        // la misma caja de eta en vez de una caja aparte.
        if (d.goal_label) {
          etaBox.classList.add('visible');
          const icon = d.goal_met ? '✅' : '⚠️';
          etaGoal.textContent = `${icon} Meta: ${d.goal_floor}% para ${d.goal_label} (proyectás ${d.goal_projected.toFixed(0)}%)`;
          etaGoal.className = 'eta-goal ' + (d.goal_met ? 'eta-ok' : 'eta-warn');
        } else {
          etaGoal.textContent = '';
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

        // Fila inferior (bottom row): CA / USB, misma fuente de datos que
        // /api/status, nunca null (siempre 0 como mínimo). extra_in_w se
        // usa para el nodo lateral consolidado (ver más abajo), no tiene
        // ícono propio en esta fila.
        const acOutW = d.ac_out_w || 0;
        const extraInW = d.extra_in_w || 0;
        const usbOutW = d.usb_out_w || 0;
        document.getElementById('ac-out-w').textContent = acOutW + ' W';
        document.getElementById('usb-out-w').textContent = usbOutW + ' W';

        // Nodo lateral consolidado (fusión de los antiguos "Extra"/arriba y
        // "Batería"/abajo en uno solo, al lado del ring): solo extra_in_w O
        // extra_out_w es distinto de cero a la vez (nunca ambos a la vez,
        // confirmado en producción), así que el nodo muestra el que esté
        // activo. Descarga (extra_out_w > piso de ruido) = ícono rojo,
        // conector rojo, animación batería->ring. Carga (extra_in_w > piso
        // de ruido) = ícono verde, conector verde, animación ring->batería.
        // Ninguno activo = gris neutro, línea estática sin animar.
        const lateralCircle = document.getElementById('lateral-circle');
        const lateralOutW = d.extra_out_w || 0;
        const lateralInW = extraInW;
        const lateralDischarging = lateralOutW > 5;
        const lateralCharging = !lateralDischarging && lateralInW > 5;
        const lateralW = lateralDischarging ? lateralOutW : lateralInW;
        const lateralState = lateralDischarging ? 'discharging' : lateralCharging ? 'charging' : 'neutral';
        document.getElementById('lateral-w').textContent = lateralW + ' W';
        lateralCircle.innerHTML = batteryIcon(lateralState);
        lateralCircle.className = 'icon-circle' + (lateralState !== 'neutral' ? ' ' + lateralState : '');
        document.getElementById('flow-lateral-charge').classList.toggle('active', lateralCharging);
        document.getElementById('flow-lateral-discharge').classList.toggle('active', lateralDischarging);

        // Overlay de flujo animado (líneas conectoras): solo se anima la
        // línea del nodo cuyo wattage actual supera el piso de ruido (5W).
        const acTopActive = (d.ac_w || 0) > 5;
        const solarActive = (d.pv_w || 0) > 5;
        const acOutActive = acOutW > 5;
        const usbActive = usbOutW > 5;
        document.getElementById('flow-ac-top').classList.toggle('active', acTopActive);
        document.getElementById('flow-solar-top').classList.toggle('active', solarActive);
        document.getElementById('flow-ac-out').classList.toggle('active', acOutActive);
        document.getElementById('flow-usb-out').classList.toggle('active', usbActive);

        function remainHtml(remain) {
          if (!remain) return '';
          const color = remain.charging ? '#4ade80' : '#f87171';
          return ` · <span style="color:${color};font-weight:600">${remain.text}</span>`;
        }

        let batHtml = '';
        if (d.soc_delta2 != null) {
          const f = batteryFlow(d.delta2_net_w);
          batHtml += `<div class="battery-row"><div class="name">${batteryIcon(f.state)}<div>Delta 2<div class="sub">${f.label}${remainHtml(d.delta2_remain)}</div></div></div><div class="val ${f.cls}">${d.soc_delta2.toFixed(1)}%${f.suffix}</div></div>`;
        }
        if (d.soc_extra != null) {
          const f = batteryFlow(d.extra_net_w);
          batHtml += `<div class="battery-row"><div class="name">${batteryIcon(f.state)}<div>Batería Extra<div class="sub">${f.label}${remainHtml(d.extra_remain)}</div></div></div><div class="val ${f.cls}">${d.soc_extra.toFixed(1)}%${f.suffix}</div></div>`;
        }
        document.getElementById('batteries').innerHTML = batHtml;

        // El fetch HTTP respondió, pero eso solo dice que el servidor está
        // vivo — no que la telemetría MQTT de la Delta 2 sea fresca. d.stale
        // viene calculado en el backend con el mismo criterio del watchdog
        // (updated_at vs WATCHDOG_STALE_MINUTES), así que un MQTT caído hace
        // horas se ve en rojo aunque el HTTP siga respondiendo.
        const liveDot = document.getElementById('live-dot');
        const staleText = document.getElementById('mqtt-stale-text');
        if (d.stale) {
          liveDot.classList.remove('ok');
          liveDot.classList.add('stale');
          staleText.textContent = d.stale_minutes != null
            ? ` · sin datos de la Delta 2 hace ${Math.round(d.stale_minutes)} min`
            : ' · sin datos de la Delta 2';
        } else {
          liveDot.classList.remove('stale');
          liveDot.classList.add('ok');
          staleText.textContent = '';
        }
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
        renderCargaEstado(d.devices);
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
            if (d.devices) { renderDevices(d.devices); renderCargaEstado(d.devices); }
          } catch (e) { /* si falla, el próximo loadDevices() corrige la vista */ }
        });
      });
    }

    // Tocable: cada badge togglea cargado/descargado (mismo patrón fetch que
    // renderDevices/.device-btn de "Qué tenés encendido"), con un caso
    // especial para ecoplay — ver handler de abajo.
    function renderCargaEstado(devices) {
      const chargeable = devices.filter(dev => dev.charged != null);
      const wrap = document.getElementById('carga-estado-wrap');
      wrap.style.display = chargeable.length ? '' : 'none';
      document.getElementById('carga-estado').innerHTML = chargeable.map(dev => `
        <div class="device-btn ${dev.charged ? 'on' : 'off'}" data-key="${dev.key}">
          <span class="name">${dev.emoji} ${dev.label}</span>
          <span class="state">${dev.charged ? '🔋 cargada' : '🪫 descargada'}</span>
        </div>
      `).join('');
      document.querySelectorAll('#carga-estado .device-btn').forEach(btn => {
        btn.addEventListener('click', async () => {
          const key = btn.dataset.key;
          const settingCharged = !btn.classList.contains('on');
          // Caso especial ecoplay: pasar a "cargada" abre el modal de % en
          // vez de togglear directo (el % es la fuente de verdad real);
          // pasar a "descargada" sí es un toggle directo (y el backend ya
          // sincroniza ECOPLAY_LAST_PCT=0 como efecto secundario).
          if (key === 'ecoplay' && settingCharged) {
            openEcoplayModal();
            return;
          }
          try {
            const res = await fetch('/api/devices/charged', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ device: key, charged: settingCharged }),
            });
            const d = await res.json();
            if (d.devices) { renderDevices(d.devices); renderCargaEstado(d.devices); }
            if (key === 'ecoplay') loadCargas();
          } catch (e) { /* si falla, el próximo loadDevices() corrige la vista */ }
        });
      });
    }

    // Mismo texto que manda el bot por Telegram (build_load_advisor_message),
    // solo se reformatea el *negrita* de Telegram a <strong> para HTML.
    function escapeHtml(s) {
      return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }
    // La meta y el "se va a cumplir" ya se muestran arriba en la caja de eta
    // (junto con "dura hasta las X") — acá se recortan del texto de
    // Gestión de cargas para no repetirlas dos veces en la misma pantalla.
    // El bot de Telegram no se toca: sigue mandando el texto completo.
    function stripMeta(msg) {
      const idx = msg.indexOf('\\n\\n🎯 Meta:');
      return idx === -1 ? msg : msg.slice(0, idx);
    }
    async function loadCargas() {
      try {
        const res = await fetch('/api/cargas');
        const d = await res.json();
        const wrap = document.getElementById('cargas-wrap');
        if (d.message) {
          wrap.style.display = 'block';
          document.getElementById('cargas-box').innerHTML =
            escapeHtml(stripMeta(d.message)).replace(/\\*(.+?)\\*/g, '<strong>$1</strong>');
        } else {
          wrap.style.display = 'none';
        }
      } catch (e) { /* silencioso, no es crítico como el estado del EcoFlow */ }
    }

    // Modal para editar el % de Ecoplay sin pasar por Telegram (POST
    // /api/ecoplay). Simplificado: solo entra un % y toca "Aceptar" — el
    // cálculo (hora segura / autonomía) YA NO se muestra acá, se ve
    // reflejado en "Gestión de cargas" en el próximo refresh (loadCargas),
    // evitando duplicar el mismo resultado en dos lugares de la pantalla.
    // Único trigger: el badge de Ecoplay en "Estado de carga" (ver
    // renderCargaEstado) — el link standalone que existía antes fue
    // removido.
    function openEcoplayModal() {
      document.getElementById('ecoplay-modal-result').innerHTML = '';
      document.getElementById('ecoplay-pct-input').value = '';
      document.getElementById('ecoplay-modal-backdrop').classList.add('visible');
    }
    function closeEcoplayModal() {
      document.getElementById('ecoplay-modal-backdrop').classList.remove('visible');
    }
    document.getElementById('ecoplay-modal-close').addEventListener('click', closeEcoplayModal);
    document.getElementById('ecoplay-modal-backdrop').addEventListener('click', (e) => {
      if (e.target.id === 'ecoplay-modal-backdrop') closeEcoplayModal();
    });
    document.getElementById('ecoplay-modal-submit').addEventListener('click', async () => {
      const resultBox = document.getElementById('ecoplay-modal-result');
      const raw = document.getElementById('ecoplay-pct-input').value;
      const pct = parseInt(raw, 10);
      if (raw === '' || isNaN(pct) || pct < 0 || pct > 100) {
        resultBox.innerHTML = '<div class="modal-result-error">Ingresá un % entero entre 0 y 100.</div>';
        return;
      }
      try {
        const res = await fetch('/api/ecoplay', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ pct })
        });
        const d = await res.json();
        if (!res.ok) {
          resultBox.innerHTML = `<div class="modal-result-error">${d.error || 'Error'}</div>`;
          return;
        }
        // Informar un % siempre implica que Ecoplay quedó "cargada" (es la
        // fuente de verdad real del estado de carga), así que sincronizamos
        // el badge de "Estado de carga" acá también, no solo el % interno.
        try {
          await fetch('/api/devices/charged', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ device: 'ecoplay', charged: true })
          });
        } catch (e2) { /* si falla, el próximo loadDevices() corrige la vista */ }
        loadCargas();
        loadDevices();
        closeEcoplayModal();
      } catch (e) {
        resultBox.innerHTML = '<div class="modal-result-error">No se pudo conectar con el servidor.</div>';
      }
    });

    refresh();
    loadDevices();
    loadCargas();
    setInterval(refresh, 2000);
    setInterval(tickClock, 1000);
    setInterval(loadDevices, 2000);
    setInterval(loadCargas, 2000);
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
        # El botón del Mini App de Telegram le pega un ?v=<timestamp> a la URL
        # para evitar el caché del lado del cliente (ver set_dashboard_menu_button)
        # — hay que cortar la query string antes de comparar la ruta, si no
        # cualquier pedido con "?" cae siempre al 404 de abajo.
        path = self.path.split("?", 1)[0]
        if path in ("/", "/index.html"):
            self._send(200, DASHBOARD_HTML.encode("utf-8"), "text/html; charset=utf-8")
        elif path == "/api/status":
            try:
                payload = json.dumps(get_dashboard_status()).encode("utf-8")
                self._send(200, payload, "application/json")
            except Exception as exc:
                payload = json.dumps({"ready": False, "error": str(exc)}).encode("utf-8")
                self._send(500, payload, "application/json")
        elif path == "/api/devices":
            payload = json.dumps(get_device_state_payload()).encode("utf-8")
            self._send(200, payload, "application/json")
        elif path == "/api/cargas":
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
        global ECOPLAY_LAST_PCT
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
        elif self.path == "/api/devices/charged":
            # Mismo contrato que POST /api/devices, pero contra DEVICE_CHARGED
            # en vez de DEVICE_STATE: body {"device": <key>, "charged": bool},
            # 400 si el dispositivo no es válido (solo ventilador1-3,
            # powerbank1-2, ecoplay), 200 con el mismo payload completo de
            # get_device_state_payload() si se aplicó.
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length) or b"{}")
                device = str(body.get("device", ""))
                if device not in DEVICE_CHARGED:
                    self._send(400, b'{"error":"dispositivo desconocido"}', "application/json")
                    return
                charged = bool(body.get("charged"))
                DEVICE_CHARGED[device] = charged
                # Mismo criterio que /descargado: Ecoplay es la única con
                # sistema de % propio, sincronizamos su % a 0 al descargarla
                # desde acá también (ventilador/powerbank no tienen % análogo).
                if device == "ecoplay" and not charged:
                    ECOPLAY_LAST_PCT = 0
                _save_persisted_state()
                payload = json.dumps(get_device_state_payload()).encode("utf-8")
                self._send(200, payload, "application/json")
            except Exception as exc:
                payload = json.dumps({"error": str(exc)}).encode("utf-8")
                self._send(500, payload, "application/json")
        elif self.path == "/api/ecoplay":
            # Contraparte web de /ecoplay <pct>: body {"pct": 0-100}, valida
            # rango (400 si inválido), setea y persiste ECOPLAY_LAST_PCT,
            # corre _ecoplay_autonomy y devuelve el mismo dict (incluye
            # has_autonomy para que el frontend distinga el caso "sin
            # autonomía todavía" del resultado normal).
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length) or b"{}")
                pct = body.get("pct")
                if not isinstance(pct, int) or isinstance(pct, bool) or not (0 <= pct <= 100):
                    self._send(400, b'{"error":"debe ser un porcentaje entero entre 0 y 100"}', "application/json")
                    return
                ECOPLAY_LAST_PCT = pct
                _save_persisted_state()
                info = _ecoplay_autonomy(ECOPLAY_LAST_PCT)
                payload = json.dumps(info).encode("utf-8")
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
