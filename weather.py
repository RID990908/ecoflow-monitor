"""Clima y horas de sol para el fondo "cielo" de la app.

Consulta Open-Meteo (gratis, sin API key) y guarda el último resultado en
memoria. NUNCA bloquea un pedido HTTP del dashboard: `get_sky()` devuelve lo
que haya en caché y, si está vencido, dispara un refresco en un hilo daemon.
Si la red falla se conserva el último valor bueno y se reintenta con espera
(`RETRY_SECONDS`) para no martillar el servicio.

Variables de entorno (todas opcionales):
  WEATHER_ENABLED      "false" apaga todo (sky=null, cero llamadas de red)
  WEATHER_LAT/LON      coordenadas (por defecto La Habana)
  WEATHER_REFRESH_SEC  segundos entre consultas (por defecto 10, mínimo 5)
La zona horaria es la de siempre (`TZ_NAME`, ver shared_state).
"""

import os
import threading
import time
from datetime import datetime

import requests

from shared_state import TZ, log

ENABLED = os.environ.get("WEATHER_ENABLED", "true").strip().lower() != "false"
LAT = float(os.environ.get("WEATHER_LAT", "23.1"))
LON = float(os.environ.get("WEATHER_LON", "-82.37"))
REFRESH_SECONDS = max(5.0, float(os.environ.get("WEATHER_REFRESH_SEC", "10")))
RETRY_SECONDS = 120.0
API_URL = "https://api.open-meteo.com/v1/forecast"

_lock = threading.Lock()
_cache = None  # último dict válido de parse_open_meteo (+ updated_ts)
_refreshing = False
_next_try = 0.0  # no reintentar antes de este instante (tras un fallo)


def parse_open_meteo(data: dict, tz) -> dict:
    """Convierte la respuesta de Open-Meteo al dict que expone /api/status.
    Lanza ValueError si falta algo (el llamador conserva el valor anterior)."""
    try:
        cur = data["current"]
        cloud = max(0, min(100, int(round(float(cur["cloud_cover"])))))
        code = int(cur["weather_code"])
        is_day = bool(int(cur["is_day"]))
        sunrise = datetime.fromisoformat(data["daily"]["sunrise"][0]).replace(tzinfo=tz)
        sunset = datetime.fromisoformat(data["daily"]["sunset"][0]).replace(tzinfo=tz)
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise ValueError(f"respuesta de clima incompleta: {exc!r}") from exc
    return {
        "cloud_pct": cloud,
        "code": code,
        "is_day": is_day,
        "sunrise_ts": sunrise.timestamp(),
        "sunset_ts": sunset.timestamp(),
    }


def _fetch() -> dict:
    resp = requests.get(
        API_URL,
        params={
            "latitude": LAT,
            "longitude": LON,
            "current": "cloud_cover,weather_code,is_day",
            "daily": "sunrise,sunset",
            "timezone": str(TZ),
            "forecast_days": 1,
        },
        timeout=6,
    )
    resp.raise_for_status()
    return parse_open_meteo(resp.json(), TZ)


def refresh() -> None:
    """Una consulta. Nunca lanza: ante un error deja la caché como estaba."""
    global _cache, _refreshing, _next_try
    try:
        sky = _fetch()
        sky["updated_ts"] = time.time()
        with _lock:
            _cache = sky
            _next_try = 0.0
    except Exception as exc:
        log.warning("No se pudo actualizar el clima: %s", exc)
        with _lock:
            _next_try = time.time() + RETRY_SECONDS
    finally:
        with _lock:
            _refreshing = False


def _spawn() -> None:
    threading.Thread(target=refresh, daemon=True).start()


def get_sky():
    """Devuelve el último clima conocido (o None) sin bloquear; si está
    vencido o no hay datos, dispara un refresco en segundo plano."""
    global _refreshing
    if not ENABLED:
        return None
    now = time.time()
    with _lock:
        sky = dict(_cache) if _cache else None
        stale = sky is None or now - sky["updated_ts"] >= REFRESH_SECONDS
        start = stale and not _refreshing and now >= _next_try
        if start:
            _refreshing = True
    if start:
        _spawn()
    return sky
