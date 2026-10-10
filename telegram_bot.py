"""Todo lo que habla con la API de Telegram: mandar mensajes, registrar el
menú de comandos y el botón del dashboard, parsear/despachar los comandos que
manda el usuario, y el long-polling que los escucha. La lógica de negocio en
sí (armar el informe, el mensaje de gestión de cargas) vive en
dashboard_server.py y se importa desde acá — así ambos "frontends" (Telegram
y el dashboard web) comparten exactamente el mismo cálculo. Extraído de
ecoflow_telegram_monitor.py al modularizar el proyecto.
"""

import os
import threading
import time

import requests

import dashboard_server
import mqtt_client
import shared_state
from shared_state import (
    AC_CHECK_MINUTES,
    BOT_TOKEN,
    CHAT_ID,
    QUIET_END_HOUR,
    QUIET_END_MINUTE,
    QUIET_START_HOUR,
    QUIET_START_MINUTE,
    log,
)

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
        {"command": "reporte", "description": "Informe detallado (Delta 2 y batería extra)"},
        {"command": "cargas", "description": "Plan de batería: meta y proyección según entrada/salida"},
        {"command": "alerta", "description": "Avisar cuando la carga baje de X% (ej: /alerta 20)"},
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


HELP_TEXT = (
    "🤖 *Monitor EcoFlow*\n\n"
    "/reporte — informe detallado (Delta 2 y batería extra)\n"
    "/cargas — plan de batería: meta y proyección según la entrada/salida actual\n"
    "/alerta <porcentaje> — avisar cuando la carga baje de ese nivel (ej: /alerta 20)\n"
    "/fuiyo — si apagaste vos la Delta 2 y te llegó el aviso de \"no recibo datos\", "
    "mandá esto para que no te siga avisando hasta que vuelva\n"
    "/start — qué hace este bot\n"
    "/help — ver esta ayuda\n\n"
    f"Informe automático cada 45 min mientras no hay corriente (pausado de {QUIET_START_HOUR:02d}:{QUIET_START_MINUTE:02d} a "
    f"{QUIET_END_HOUR:02d}:{QUIET_END_MINUTE:02d}) — incluye el detalle de batería/puertos y, en el mismo mensaje, "
    "el plan de batería (meta y proyección). Chequeo de carga AC cada "
    f"{AC_CHECK_MINUTES:g} min, también te aviso al llegar a 100% de carga.\n\n"
    "⚠️ Y si el ritmo de descarga proyecta que vas a llegar corto a la meta "
    "(65-75% a las 3 PM, 100% al anochecer si te mantenés en internet nomás), "
    "te aviso antes de que pase."
)
START_TEXT = "👋 Hola, soy el monitor de tu EcoFlow.\n\n" + HELP_TEXT


def handle_command(text: str, chat_id: str) -> None:
    parts = text.strip().split()
    cmd = parts[0].split("@")[0].lower()
    if cmd == "/reporte":
        send_telegram(dashboard_server.build_report(), chat_id=chat_id)
    elif cmd == "/cargas":
        msg = dashboard_server.build_battery_plan_message()
        if not msg:
            msg = (
                "🌙 Fuera de franja (6:00 AM–12:00 AM): sin plan de batería."
            )
        send_telegram(msg, chat_id=chat_id)
    elif cmd == "/alerta":
        if len(parts) < 2 or not parts[1].isdigit() or not (0 <= int(parts[1]) <= 100):
            send_telegram("Uso: /alerta <porcentaje entre 0 y 100>, ej: /alerta 20", chat_id=chat_id)
        else:
            shared_state.BATTERY_LOW_THRESHOLD = int(parts[1])
            shared_state._save_persisted_state()
            send_telegram(f"🔔 Te voy a avisar cuando la carga baje de {shared_state.BATTERY_LOW_THRESHOLD}%.", chat_id=chat_id)
    elif cmd == "/fuiyo":
        is_stale, _ = mqtt_client._mqtt_staleness()
        if not is_stale:
            send_telegram("No hay ningún corte activo ahora mismo, no hace falta.", chat_id=chat_id)
        elif shared_state.STALE_ACK_BY_USER:
            send_telegram("Ya sabía, sigo sin avisarte hasta que vuelva.", chat_id=chat_id)
        else:
            shared_state.STALE_ACK_BY_USER = True
            shared_state._save_persisted_state()
            send_telegram(
                "👍 Anotado. No te aviso más de este corte — sigo chequeando solo, "
                "y en cuanto vuelva a recibir datos te confirmo.",
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


def _delete_telegram_message(message_id: int) -> None:
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage",
            json={"chat_id": CHAT_ID, "message_id": message_id},
            timeout=15,
        )
    except Exception:
        log.exception("No se pudo borrar el mensaje %s", message_id)
