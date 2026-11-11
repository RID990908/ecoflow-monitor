"""Configuración leída de variables de entorno + estado persistido compartido
entre módulos (ecoflow_api, mqtt_client, dashboard_server, telegram_bot y el
entrypoint). Extraído de ecoflow_telegram_monitor.py al modularizar el
proyecto: antes todo esto vivía como globals de un único archivo; ahora que
las funciones que los leen/escriben están repartidas en varios módulos, este
es el único lugar donde se definen para que todos apunten al mismo estado.

IMPORTANTE para quien toque este archivo: los escalares que cambian en
tiempo de ejecución (WAS_CHARGING_AC, AC_ORIGIN, GRID_OUT, BATTERY_LOW_THRESHOLD,
WAS_BELOW_LOW_THRESHOLD, WAS_FULL, LAST_AC_TIMESTAMP, OUTAGE_START_TIMESTAMP,
_DATA_STALE_ALERTED, STALE_ACK_BY_USER) deben leerse/escribirse siempre como
`shared_state.NOMBRE` (atributo del módulo) desde los demás archivos, NUNCA
con `from shared_state import NOMBRE` — un import directo copia el valor de
en ese momento y no ve las actualizaciones que haga otro módulo (ej.
telegram_bot cambia BATTERY_LOW_THRESHOLD con /alerta y dashboard_server
necesita ver ese cambio en la próxima consulta).
"""

import json
import logging
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

# Railway corre en UTC; esto es lo que usan tanto los timestamps de cara al
# usuario (ETA de autonomía, bloques del plan de cargas) como el formatter de
# logging de acá abajo, para que ambos coincidan. zoneinfo maneja el horario
# de verano de Cuba automáticamente.
TZ = ZoneInfo(os.environ.get("TZ_NAME", "America/Havana"))


class _HavanaFormatter(logging.Formatter):
    """Railway corre en UTC; sin esto %(asctime)s salía en hora del
    contenedor mientras todos los timestamps de cara al usuario (ETA de
    autonomía, bloques del plan) usan datetime.now(TZ) — mismo desfase de
    horas que causaba confusión al comparar logs contra lo que reporta el
    bot. logging.Formatter.converter espera time.struct_time, no permite
    zoneinfo directo, así que se arma vía datetime.fromtimestamp(tz=TZ)."""

    def converter(self, timestamp):
        return datetime.fromtimestamp(timestamp, TZ).timetuple()


_log_handler = logging.StreamHandler()
_log_handler.setFormatter(_HavanaFormatter("%(asctime)s [%(levelname)s] %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[_log_handler])
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
AC_WATTS_THRESHOLD = 5  # piso de ruido en watts: por debajo de esto se considera "no hay carga/consumo real", ya sea para el texto de la alerta de AC en Telegram o para los watts netos que muestra dashboard_server (ver _gather_metrics)

# Horario silencioso: de noche no tiene sentido recibir el informe automático
# cada media hora. Las alertas (llegó/se fue la luz, batería baja/llena) siguen
# funcionando igual, solo se pausa el informe periódico.
QUIET_START_HOUR = int(os.environ.get("QUIET_START_HOUR", "23"))
QUIET_START_MINUTE = int(os.environ.get("QUIET_START_MINUTE", "30"))
QUIET_END_HOUR = int(os.environ.get("QUIET_END_HOUR", "7"))
QUIET_END_MINUTE = int(os.environ.get("QUIET_END_MINUTE", "0"))

# Plan de la planta eléctrica (plant_plan del dashboard): velocidad de carga
# por AC de la EcoFlow (W; la planta entrega lo que la EcoFlow le pide), hora local desde la que se calcula el plan y
# horas silenciosas en las que la planta nunca corre (22:00-08:00; solo se
# puede usar de 08:00 a 22:00).
AC_CHARGE_W = float(os.environ.get("AC_CHARGE_W", "1200"))
PLANT_PLAN_FROM_HOUR = 8
PLANT_QUIET_START_HOUR = 22
PLANT_QUIET_END_HOUR = 8

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
                    "outage_start_timestamp": OUTAGE_START_TIMESTAMP,
                    "data_stale_alerted": _DATA_STALE_ALERTED,
                    "stale_ack_by_user": STALE_ACK_BY_USER,
                    "ac_origin": AC_ORIGIN,
                    "grid_out": GRID_OUT,
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
# Se setea cuando se detecta que se fue la luz (AC presente -> ausente) y se
# limpia (vuelve a None) cuando vuelve, para no arrastrar el corte anterior —
# ver ac_check_timer() en ecoflow_telegram_monitor.py. Mismo patrón in-memory
# (sin persistencia propia más allá del snapshot general de acá) que
# LAST_AC_TIMESTAMP: sobrevive redeploys porque va en el mismo STATE_FILE.
OUTAGE_START_TIMESTAMP = _persisted.get("outage_start_timestamp")
_DATA_STALE_ALERTED = _persisted.get("data_stale_alerted", False)
STALE_ACK_BY_USER = _persisted.get("stale_ack_by_user", False)
# Origen de la CA marcado por el usuario desde la app: "unclassified" (aún no
# dijo), "grid" (red de la calle) o "plant" (planta de gas). Es solo una
# etiqueta: la detección de que hay AC es por voltaje y nunca espera esto.
# GRID_OUT: la red de la calle se cortó, así que la próxima CA que llegue se
# asume de la planta hasta que el usuario diga que volvió la red.
AC_ORIGIN = _persisted.get("ac_origin", "unclassified")
GRID_OUT = _persisted.get("grid_out", False)
