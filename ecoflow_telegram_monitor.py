#!/usr/bin/env python3
"""
Monitor EcoFlow (Delta 2 + batería extra) → informe por Telegram cada 2 horas.

Variables de entorno requeridas:
  ECOFLOW_ACCESS_KEY   Access key de developer.ecoflow.com
  ECOFLOW_SECRET_KEY   Secret key de developer.ecoflow.com
  ECOFLOW_SN_DELTA2    Número de serie de la Delta 2
  ECOFLOW_SN_EXTRA     Número de serie de la batería extra (opcional)
  TELEGRAM_BOT_TOKEN   Token del bot (de @BotFather)
  TELEGRAM_CHAT_ID     Chat ID destino (de @userinfobot)
  INTERVAL_HOURS       Intervalo en horas (default: 2)

Uso:
  python3 ecoflow_telegram_monitor.py           # loop continuo cada N horas
  python3 ecoflow_telegram_monitor.py --once    # una sola ejecución (para cron)
"""

import hashlib
import hmac
import logging
import os
import random
import sys
import threading
import time
from datetime import datetime

import requests

# El "accessKey is invalid" que aparecía al principio no era un problema de
# región: era que la key recién generada tarda unos minutos en activarse en
# EcoFlow. api.ecoflow.com funciona bien. Queda configurable por las dudas.
API_HOST = os.environ.get("ECOFLOW_API_HOST", "https://api.ecoflow.com")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ecoflow-monitor")


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        log.error("Falta la variable de entorno %s", name)
        sys.exit(1)
    return value


# Las credenciales de EcoFlow son opcionales al arrancar: el bot debe poder
# responder /start y /help aunque todavía no estén (p. ej. mientras se espera
# la aprobación del programa developer). Se validan recién al armar el reporte.
ACCESS_KEY = os.environ.get("ECOFLOW_ACCESS_KEY", "").strip()
SECRET_KEY = os.environ.get("ECOFLOW_SECRET_KEY", "").strip()
SN_DELTA2 = os.environ.get("ECOFLOW_SN_DELTA2", "").strip()
SN_EXTRA = os.environ.get("ECOFLOW_SN_EXTRA", "").strip()
BOT_TOKEN = require_env("TELEGRAM_BOT_TOKEN")
CHAT_ID = require_env("TELEGRAM_CHAT_ID")
INTERVAL_HOURS = float(os.environ.get("INTERVAL_HOURS", "2"))
ECOFLOW_READY = bool(ACCESS_KEY and SECRET_KEY and SN_DELTA2)
START_TIME = time.time()
AUTO_PAUSED = False
AC_CHECK_MINUTES = float(os.environ.get("AC_CHECK_MINUTES", "30"))
AC_WATTS_THRESHOLD = 5  # por debajo de esto se considera "no está cargando por AC"
if not ECOFLOW_READY:
    log.warning(
        "EcoFlow no configurado todavía (faltan ACCESS_KEY/SECRET_KEY/SN_DELTA2); "
        "el bot responde comandos de Telegram pero /reporte no va a poder consultar el dispositivo."
    )


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
    signature = hmac.new(
        SECRET_KEY.encode("utf-8"), sign_str.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    return {
        "accessKey": ACCESS_KEY,
        "nonce": nonce,
        "timestamp": timestamp,
        "sign": signature,
        "Content-Type": "application/json;charset=UTF-8",
    }


def get_device_quota(sn: str) -> dict:
    params = {"sn": sn}
    headers = _signed_headers(params)
    resp = requests.get(
        f"{API_HOST}/iot-open/sign/device/quota/all",
        params=params,
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    if str(payload.get("code")) != "0":
        raise RuntimeError(f"EcoFlow API error para {sn}: {payload}")
    return payload.get("data", {})


def get_device_online(sn: str) -> bool:
    headers = _signed_headers({})
    resp = requests.get(
        f"{API_HOST}/iot-open/sign/device/list", headers=headers, timeout=30
    )
    resp.raise_for_status()
    payload = resp.json()
    if str(payload.get("code")) != "0":
        raise RuntimeError(f"EcoFlow API error (device list): {payload}")
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
    pv_w = _pick(data, "mppt.inWatts")
    return round(pv_w / 10, 1) if pv_w is not None else None  # viene en décimas de watt


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
        f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands",
        json={"commands": commands},
        timeout=30,
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
    """Línea compacta y combinada: carga de ambas baterías + entrada solar. Para /estado."""
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

    combined = _combined_line(soc_delta2, soc_extra)
    if combined:
        parts.append(f"Total {round((soc_delta2 + soc_extra) / 2, 1)}%")

    return "🔋 " + " · ".join(parts)


def run_once() -> None:
    report = build_report()
    send_telegram(report)
    log.info("Informe enviado")


HELP_TEXT = (
    "🤖 *Monitor EcoFlow*\n\n"
    "/reporte — informe detallado, por dispositivo (Delta 2 y batería extra)\n"
    "/estado — línea rápida combinada (carga de ambas + solar)\n"
    "/pausa — pausar los informes automáticos\n"
    "/reanudar — reanudar los informes automáticos\n"
    "/ping — ver si el bot está activo\n"
    "/start — qué hace este bot\n"
    "/help — ver esta ayuda\n\n"
    f"Reporte automático cada {INTERVAL_HOURS:g}h (si no está pausado).\n"
    f"⚡ Además te aviso apenas empiece a cargar por corriente (chequeo cada {AC_CHECK_MINUTES:g} min)."
)
START_TEXT = (
    "👋 Hola, soy el monitor de tu EcoFlow.\n"
    f"Te mando un reporte automático cada {INTERVAL_HOURS:g}h con la carga de la "
    "Delta 2, la batería extra y la entrada solar.\n\n" + HELP_TEXT
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


def watch_ac_charging() -> None:
    """Avisa apenas la Delta 2 empieza a cargar por corriente (AC), sin esperar al reporte periódico."""
    was_charging = False
    while True:
        if not ECOFLOW_READY or AUTO_PAUSED:
            time.sleep(AC_CHECK_MINUTES * 60)
            continue
        try:
            data = get_device_quota(SN_DELTA2)
            ac_w = get_ac_watts(data)
            is_charging = bool(ac_w and ac_w > AC_WATTS_THRESHOLD)
            if is_charging and not was_charging:
                send_telegram(f"⚡ Llegó la corriente: la Delta 2 empezó a cargar por AC ({ac_w} W).")
                log.info("Notificado inicio de carga AC (%s W)", ac_w)
            was_charging = is_charging
        except Exception:
            log.exception("Error chequeando carga por AC; reintento en el próximo ciclo")
        time.sleep(AC_CHECK_MINUTES * 60)


def main() -> None:
    if "--once" in sys.argv:
        run_once()
        return

    try:
        set_bot_commands()
    except Exception:
        log.exception("No se pudo registrar el menú de comandos (no bloqueante)")

    poll_thread = threading.Thread(target=poll_commands, daemon=True)
    poll_thread.start()

    ac_thread = threading.Thread(target=watch_ac_charging, daemon=True)
    ac_thread.start()

    interval_s = INTERVAL_HOURS * 3600
    log.info("Iniciando monitoreo cada %.1f horas", INTERVAL_HOURS)
    while True:
        if not ECOFLOW_READY:
            log.info("EcoFlow no configurado; se omite el informe automático de este ciclo")
        elif AUTO_PAUSED:
            log.info("Informes automáticos pausados; se omite este ciclo")
        else:
            try:
                run_once()
            except Exception:
                log.exception("Fallo en el ciclo de informe; reintento en el próximo ciclo")
                try:
                    send_telegram("⚠️ Fallo al generar el informe EcoFlow. Revisa los logs.")
                except Exception:
                    log.exception("Tampoco se pudo notificar el error por Telegram")
        time.sleep(interval_s)


if __name__ == "__main__":
    main()
