"""Cliente HTTP para la API de EcoFlow: firma/auth de la API oficial de
developer.ecoflow.com (hoy dormida, ver nota más abajo) y login/credenciales
MQTT de la "private API" (la que realmente se usa en producción). Extraído
de ecoflow_telegram_monitor.py al modularizar el proyecto.
"""

import base64
import hashlib
import hmac
import os
import random
import time

import requests

from shared_state import ACCESS_KEY, ECOFLOW_EMAIL, ECOFLOW_PASSWORD, SECRET_KEY, log

API_HOST = os.environ.get("ECOFLOW_API_HOST", "https://api.ecoflow.com")


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


# NOTA: _ecoflow_get (y _signed_headers/_flatten/_refresh_proxy_pool que la
# soportan) son la API OFICIAL de developer.ecoflow.com — hoy sin uso real en
# producción porque EcoFlow nunca habilitó el permiso del dispositivo en esa
# cuenta developer (ver README, sección "Por qué 'Private API' y no la API
# oficial de developer"). Se dejan en el código, dormidas, por si el día de
# mañana EcoFlow resuelve el permiso y conviene volver a la API oficial.
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
