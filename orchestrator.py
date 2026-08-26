#!/usr/bin/env python3
"""
Orquestador del bot EcoFlow → Telegram. Corre 24/7 en Railway (no tiene el
piso de 5 min de cron que tiene GitHub Actions), escucha comandos de
Telegram al instante, y decide CUÁNDO disparar cada acción real. Las
acciones que necesitan hablar con la API de EcoFlow (reporte, estado,
chequeo de carga AC) NO las ejecuta este proceso — las delega a un workflow
de GitHub Actions vía workflow_dispatch, porque EcoFlow bloquea las IPs de
Railway pero no las de GitHub Actions. El worker de GitHub Actions manda la
respuesta a Telegram directamente, sin volver a pasar por acá.

Variables de entorno requeridas:
  TELEGRAM_BOT_TOKEN    Token del bot
  TELEGRAM_CHAT_ID      Chat ID destino
  GITHUB_TOKEN          Personal Access Token con permiso "repo" (Actions: write)
  GITHUB_REPO           owner/repo, ej. RID990908/ecoflow-monitor
  GITHUB_WORKFLOW       Nombre del archivo del workflow, ej. worker.yml
  GITHUB_REF            Branch a usar (default: master)
  INTERVAL_HOURS        Cada cuánto disparar el informe automático (default: 1)
  AC_CHECK_MINUTES      Cada cuánto disparar el chequeo de carga AC (default: 1)
"""

import logging
import os
import sys
import threading
import time

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("orchestrator")


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        log.error("Falta la variable de entorno %s", name)
        sys.exit(1)
    return value


BOT_TOKEN = require_env("TELEGRAM_BOT_TOKEN")
CHAT_ID = require_env("TELEGRAM_CHAT_ID")
GITHUB_TOKEN = require_env("GITHUB_TOKEN")
GITHUB_REPO = require_env("GITHUB_REPO")
GITHUB_WORKFLOW = os.environ.get("GITHUB_WORKFLOW_FILE", "worker.yml")
GITHUB_REF = os.environ.get("GITHUB_REF_NAME", "master")
INTERVAL_HOURS = float(os.environ.get("INTERVAL_HOURS", "1"))
AC_CHECK_MINUTES = float(os.environ.get("AC_CHECK_MINUTES", "1"))

START_TIME = time.time()
AUTO_PAUSED = False


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
        {"command": "reporte", "description": "Informe detallado por dispositivo (~20-40s)"},
        {"command": "estado", "description": "Línea rápida combinada (~20-40s)"},
        {"command": "pausa", "description": "Pausar los informes automáticos"},
        {"command": "reanudar", "description": "Reanudar los informes automáticos"},
        {"command": "ping", "description": "Ver si el orquestador está activo"},
        {"command": "help", "description": "Ver comandos disponibles"},
    ]
    resp = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands",
        json={"commands": commands},
        timeout=30,
    )
    resp.raise_for_status()


def dispatch_action(action: str) -> bool:
    """Dispara el workflow de GitHub Actions que ejecuta la acción contra EcoFlow."""
    resp = requests.post(
        f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{GITHUB_WORKFLOW}/dispatches",
        headers={
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json",
        },
        json={"ref": GITHUB_REF, "inputs": {"action": action}},
        timeout=20,
    )
    if resp.status_code >= 300:
        log.error("Fallo al disparar acción %s: %s %s", action, resp.status_code, resp.text)
        return False
    log.info("Acción '%s' disparada en GitHub Actions", action)
    return True


HELP_TEXT = (
    "🤖 *Monitor EcoFlow*\n\n"
    "/reporte — informe detallado, por dispositivo (tarda ~20-40s, lo procesa GitHub Actions)\n"
    "/estado — línea rápida combinada (tarda ~20-40s)\n"
    "/pausa — pausar los informes e chequeos automáticos\n"
    "/reanudar — reanudarlos\n"
    "/ping — ver si el orquestador está activo\n"
    "/start — qué hace este bot\n"
    "/help — ver esta ayuda\n\n"
    f"Informe automático cada {INTERVAL_HOURS:g}h · chequeo de carga AC cada {AC_CHECK_MINUTES:g} min "
    "(si no está pausado)."
)
START_TEXT = (
    "👋 Hola, soy el monitor de tu EcoFlow.\n"
    "Los comandos que hablan con EcoFlow (/reporte, /estado) tardan unos segundos porque "
    "se procesan en GitHub Actions, no acá — Railway solo orquesta.\n\n" + HELP_TEXT
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
        if dispatch_action("report"):
            send_telegram("🔄 Consultando EcoFlow, te contesto en un momento...", chat_id=chat_id)
        else:
            send_telegram("⚠️ No pude disparar la consulta. Reintentá en un rato.", chat_id=chat_id)
    elif cmd == "/estado":
        if dispatch_action("estado"):
            send_telegram("🔄 Consultando EcoFlow, te contesto en un momento...", chat_id=chat_id)
        else:
            send_telegram("⚠️ No pude disparar la consulta. Reintentá en un rato.", chat_id=chat_id)
    elif cmd == "/pausa":
        AUTO_PAUSED = True
        send_telegram("⏸ Informes y chequeos automáticos pausados. Usá /reanudar para reactivarlos.", chat_id=chat_id)
    elif cmd == "/reanudar":
        AUTO_PAUSED = False
        send_telegram("▶️ Informes y chequeos automáticos reanudados.", chat_id=chat_id)
    elif cmd == "/ping":
        estado_auto = "⏸ pausado" if AUTO_PAUSED else "▶️ activo"
        send_telegram(
            f"🏓 Pong. Orquestador activo hace {_format_uptime()}.\nAutomático: {estado_auto}",
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
        if AUTO_PAUSED:
            log.info("Informe automático omitido (pausado)")
            continue
        dispatch_action("report")


def ac_check_timer() -> None:
    while True:
        time.sleep(AC_CHECK_MINUTES * 60)
        if AUTO_PAUSED:
            continue
        dispatch_action("ac_check")


def main() -> None:
    try:
        set_bot_commands()
    except Exception:
        log.exception("No se pudo registrar el menú de comandos (no bloqueante)")

    threading.Thread(target=poll_commands, daemon=True).start()
    threading.Thread(target=report_timer, daemon=True).start()
    threading.Thread(target=ac_check_timer, daemon=True).start()

    log.info(
        "Orquestador iniciado. Informe cada %.1fh, chequeo AC cada %.1f min.",
        INTERVAL_HOURS, AC_CHECK_MINUTES,
    )
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
