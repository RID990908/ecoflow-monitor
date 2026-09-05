"""Configuración leída de variables de entorno + estado persistido compartido
entre módulos (ecoflow_api, mqtt_client, dashboard_server, telegram_bot y el
entrypoint). Extraído de ecoflow_telegram_monitor.py al modularizar el
proyecto: antes todo esto vivía como globals de un único archivo; ahora que
las funciones que los leen/escriben están repartidas en varios módulos, este
es el único lugar donde se definen para que todos apunten al mismo estado.

IMPORTANTE para quien toque este archivo: los escalares que cambian en
tiempo de ejecución (WAS_CHARGING_AC, BATTERY_LOW_THRESHOLD,
WAS_BELOW_LOW_THRESHOLD, WAS_FULL, LAST_AC_TIMESTAMP, ECOPLAY_LAST_PCT,
_DATA_STALE_ALERTED, STALE_ACK_BY_USER) deben leerse/escribirse siempre como
`shared_state.NOMBRE` (atributo del módulo) desde los demás archivos, NUNCA
con `from shared_state import NOMBRE` — un import directo copia el valor de
en ese momento y no ve las actualizaciones que haga otro módulo (ej.
telegram_bot cambia BATTERY_LOW_THRESHOLD con /alerta y dashboard_server
necesita ver ese cambio en la próxima consulta). DEVICE_STATE y
DEVICE_CHARGED son la excepción: son dicts que solo se mutan por clave
(`DEVICE_STATE[key] = ...`), nunca se reasignan enteros, así que un
`from shared_state import DEVICE_STATE` sí es seguro (todos comparten el
mismo objeto dict).
"""

import json
import logging
import os
import sys
from zoneinfo import ZoneInfo

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
AC_WATTS_THRESHOLD = 5  # por debajo de esto se considera "no está cargando por AC"; solo mide el puerto AC para el texto de la alerta de Telegram

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
                    "data_stale_alerted": _DATA_STALE_ALERTED,
                    "stale_ack_by_user": STALE_ACK_BY_USER,
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
_DATA_STALE_ALERTED = _persisted.get("data_stale_alerted", False)
STALE_ACK_BY_USER = _persisted.get("stale_ack_by_user", False)
