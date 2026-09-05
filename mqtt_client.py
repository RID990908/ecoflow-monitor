"""Cliente MQTT privado de EcoFlow (mismo canal que usa la app móvil): login,
conexión persistente, suscripción a los topics de la Delta 2 / batería extra,
y el caché de telemetría que alimenta tanto el bot de Telegram como el
dashboard. También expone get_device_quota/get_device_quota_cached, que
deciden si leer ese caché MQTT o pegarle a la API oficial según el modo
configurado. Extraído de ecoflow_telegram_monitor.py al modularizar el
proyecto.
"""

import json
import threading
import time
import uuid as uuid_lib

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

import ecoflow_api
from shared_state import SN_DELTA2, SN_EXTRA, USE_PRIVATE_API, log

# --- Estado del cliente MQTT privado (solo si USE_PRIVATE_API) ---
_mqtt_client = None
_mqtt_user_id = None
_device_cache = {}  # sn -> {"quota": {...}, "online": bool, "updated_at": ts}
_mqtt_cache_lock = threading.Lock()


def _mqtt_topics(sn: str) -> dict:
    return {
        "get": f"/app/{_mqtt_user_id}/{sn}/thing/property/get",
        "get_reply": f"/app/{_mqtt_user_id}/{sn}/thing/property/get_reply",
        "data": f"/app/device/property/{sn}",
    }


def _request_quota_refresh(sn: str) -> None:
    if _mqtt_client is None:
        return
    topics = _mqtt_topics(sn)
    payload = json.dumps({"version": "1.1", "moduleType": 0, "operateType": "latestQuotas", "params": {}})
    _mqtt_client.publish(topics["get"], payload)


_bms_slave_last_seen = {}  # {sn: timestamp} última vez que llegó algún campo bms_slave.* real


def _mark_bms_slave_seen(sn: str, fields: dict) -> None:
    # La batería extra no tiene su propio topic MQTT: sus datos (bms_slave.*)
    # vienen mezclados en el mismo mensaje que el resto de la Delta 2. Si se
    # desconecta físicamente, el dispositivo deja de incluir esas claves por
    # completo (no manda un 0 explícito) — el merge de _device_cache no
    # expira nunca, así que sin este tracking aparte, un valor viejo (ej.
    # 20W cargando) queda pegado para siempre aunque la batería ya no esté.
    if any(k.startswith("bms_slave.") for k in fields):
        _bms_slave_last_seen[sn] = time.time()


def _on_mqtt_message(client, userdata, msg):
    try:
        raw = json.loads(msg.payload.decode("utf-8", errors="ignore"))
    except Exception:
        log.warning("Payload MQTT invalido en %s, descartado: %.200s", msg.topic, msg.payload)
        return
    for sn in (SN_DELTA2, SN_EXTRA):
        if not sn:
            continue
        topics = _mqtt_topics(sn)
        with _mqtt_cache_lock:
            if msg.topic == topics["data"]:
                fields = raw.get("params", raw)
                entry = _device_cache.setdefault(sn, {})
                entry.setdefault("quota", {}).update(fields)
                entry["updated_at"] = time.time()
                _mark_bms_slave_seen(sn, fields)
            elif msg.topic == topics["get_reply"] and raw.get("operateType") == "latestQuotas":
                d = raw.get("data", {})
                entry = _device_cache.setdefault(sn, {})
                entry["online"] = bool(d.get("online"))
                if d.get("online") and "quotaMap" in d:
                    entry.setdefault("quota", {}).update(d["quotaMap"])
                    _mark_bms_slave_seen(sn, d["quotaMap"])
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
            token, user_id = ecoflow_api._private_login()
            _mqtt_user_id = user_id
            url, port, username, password = ecoflow_api._private_mqtt_creds(token)
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

    payload = ecoflow_api._ecoflow_get("/iot-open/sign/device/quota/all", {"sn": sn})
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


WATCHDOG_STALE_MINUTES = 5  # sin datos frescos por más de esto = alerta


def _mqtt_staleness() -> tuple:
    """(is_stale, minutos_sin_dato). Mismo criterio en watchdog_timer, /api/status
    y /fuiyo — antes estaba duplicado entre los primeros dos, ahora es uno solo."""
    with _mqtt_cache_lock:
        entry = _device_cache.get(SN_DELTA2, {})
        updated_at = entry.get("updated_at", 0)
    stale_for_min = (time.time() - updated_at) / 60 if updated_at else None
    is_stale = stale_for_min is None or stale_for_min > WATCHDOG_STALE_MINUTES
    return is_stale, stale_for_min
