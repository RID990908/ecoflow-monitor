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

Este archivo es el entrypoint/orquestador: junta la configuración y el
estado compartido (shared_state.py), el cliente MQTT/API de EcoFlow
(mqtt_client.py, ecoflow_api.py), la lógica de métricas + dashboard web
(dashboard_server.py) y el bot de Telegram (telegram_bot.py), y arranca los
threads daemon que corren 24/7. Los timers que cruzan más de un módulo
(chequeo de AC, watchdog de datos, proyección de batería, limpieza semanal)
viven acá porque son pura orquestación: leen métricas de dashboard_server,
estado de mqtt_client y mandan avisos con telegram_bot.
"""

import os
import threading
import time
from datetime import datetime, timedelta

import dashboard_server
import mqtt_client
import shared_state
import telegram_bot
from shared_state import SN_DELTA2, TZ, log

# --- Informe automático a las :00/:30, pausado en horario silencioso ---


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


_QUIET_START_MIN = shared_state.QUIET_START_HOUR * 60 + shared_state.QUIET_START_MINUTE
_QUIET_END_MIN = shared_state.QUIET_END_HOUR * 60 + shared_state.QUIET_END_MINUTE


def _in_quiet_hours(now=None) -> bool:
    now = now or datetime.now(TZ)
    minute_of_day = now.hour * 60 + now.minute
    if _QUIET_START_MIN > _QUIET_END_MIN:  # el rango cruza la medianoche
        return minute_of_day >= _QUIET_START_MIN or minute_of_day < _QUIET_END_MIN
    return _QUIET_START_MIN <= minute_of_day < _QUIET_END_MIN


def report_timer() -> None:
    while True:
        time.sleep(_seconds_until_next_slot())
        if not shared_state.ECOFLOW_READY:
            log.info("Informe automático omitido (EcoFlow no configurado)")
            continue
        if _in_quiet_hours():
            log.info("Informe automático omitido (horario silencioso)")
            continue
        try:
            telegram_bot.send_telegram(dashboard_server.build_combined_message())
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
        if not shared_state.ECOFLOW_READY:
            continue
        now_quiet = _in_quiet_hours()
        try:
            if now_quiet and not _quiet_mode_active:
                telegram_bot.send_telegram(
                    f"🌙 Entrando en horario silencioso ({shared_state.QUIET_START_HOUR:02d}:{shared_state.QUIET_START_MINUTE:02d}–"
                    f"{shared_state.QUIET_END_HOUR:02d}:{shared_state.QUIET_END_MINUTE:02d}): pauso los informes automáticos hasta las "
                    f"{shared_state.QUIET_END_HOUR:02d}:{shared_state.QUIET_END_MINUTE:02d}. Las alertas siguen activas."
                )
                _quiet_mode_active = True
                log.info("Horario silencioso activado")
            elif not now_quiet and _quiet_mode_active:
                telegram_bot.send_telegram("☀️ Salgo del horario silencioso, retoman los informes automáticos.")
                _quiet_mode_active = False
                log.info("Horario silencioso desactivado")
        except Exception:
            log.exception("Error en el aviso de horario silencioso")


WEEKLY_CLEANUP_WEEKDAY = int(os.environ.get("WEEKLY_CLEANUP_WEEKDAY", "6"))  # 0=lunes … 6=domingo
WEEKLY_CLEANUP_HOUR = int(os.environ.get("WEEKLY_CLEANUP_HOUR", "4"))
_last_cleanup_week = None


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
        with telegram_bot._sent_message_lock:
            ids = list(telegram_bot._sent_message_ids)
            telegram_bot._sent_message_ids.clear()
        for msg_id in ids:
            telegram_bot._delete_telegram_message(msg_id)
        _last_cleanup_week = week_key
        log.info("Limpieza semanal: %d mensajes borrados", len(ids))
        try:
            telegram_bot.send_telegram("🧹 Limpieza semanal del chat hecha.")
        except Exception:
            log.exception("Error avisando la limpieza semanal")


FULL_CHARGE_THRESHOLD = 99  # % a partir del cual se considera "carga completa"
FULL_CHARGE_RESET_THRESHOLD = 95  # baja de esto para poder volver a avisar


def ac_check_timer() -> None:
    """Chequea, en un mismo ciclo: si llegó la corriente, si la carga bajó del
    umbral configurado con /alerta, y si terminó de cargar (100%)."""
    while True:
        time.sleep(shared_state.AC_CHECK_MINUTES * 60)
        if not shared_state.ECOFLOW_READY:
            continue
        try:
            data = mqtt_client.get_device_quota(SN_DELTA2)
            pv_w = dashboard_server.get_pv_watts(data)
            ac_w, _ = dashboard_server.classify_ac_and_battery_watts(data, pv_w)
            # Presencia real de AC (por voltaje), no por wattage neto — así no
            # se dispara "se fue la luz" cuando la batería está llena y el AC
            # sigue enchufado en paso-directo (0W netos pero AC presente).
            is_charging = dashboard_server.get_ac_present(data)
            if is_charging and not shared_state.WAS_CHARGING_AC:
                if ac_w > shared_state.AC_WATTS_THRESHOLD:
                    telegram_bot.send_telegram(f"⚡ Llegó la corriente: la Delta 2 empezó a cargar por AC ({ac_w} W).")
                else:
                    telegram_bot.send_telegram("⚡ Llegó la corriente (la batería ya está llena, no está cargando neto).")
                log.info("Notificado inicio de AC (%s W)", ac_w)
                shared_state.LAST_AC_TIMESTAMP = time.time()
            elif not is_charging and shared_state.WAS_CHARGING_AC:
                telegram_bot.send_telegram("🔌⚠️ Se fue la luz: la Delta 2 dejó de tener AC conectado.")
                log.info("Notificado corte de luz (AC desconectado)")

            soc = dashboard_server._pick(data, "bms_bmsStatus.f32ShowSoc", "bms_bmsStatus.soc", "pd.soc")
            state_changed = is_charging != shared_state.WAS_CHARGING_AC
            shared_state.WAS_CHARGING_AC = is_charging

            if soc is not None:
                is_below = soc < shared_state.BATTERY_LOW_THRESHOLD
                if is_below and not shared_state.WAS_BELOW_LOW_THRESHOLD:
                    telegram_bot.send_telegram(f"🪫 La carga bajó de {shared_state.BATTERY_LOW_THRESHOLD}% (ahora {soc:.1f}%).")
                    log.info("Notificada carga baja (%.1f%%)", soc)
                if is_below != shared_state.WAS_BELOW_LOW_THRESHOLD:
                    shared_state.WAS_BELOW_LOW_THRESHOLD = is_below
                    state_changed = True

                is_full = soc >= FULL_CHARGE_THRESHOLD
                if is_full and not shared_state.WAS_FULL:
                    telegram_bot.send_telegram(f"🔋 La Delta 2 terminó de cargar ({soc:.1f}%).")
                    log.info("Notificada carga completa (%.1f%%)", soc)
                    shared_state.WAS_FULL = True
                    state_changed = True
                elif not is_full and soc < FULL_CHARGE_RESET_THRESHOLD and shared_state.WAS_FULL:
                    shared_state.WAS_FULL = False
                    state_changed = True

            if state_changed:
                shared_state._save_persisted_state()
        except Exception:
            log.exception("Error chequeando carga por AC")


WATCHDOG_CHECK_MINUTES = 5


def watchdog_timer() -> None:
    """Avisa si dejamos de recibir datos del dispositivo por MQTT (conexión
    caída, credenciales vencidas, etc.) — sin esto, un corte silencioso solo
    se nota cuando el usuario nota que dejaron de llegar informes.

    Si el usuario apagó la planta a propósito, /fuiyo pone
    STALE_ACK_BY_USER=True: acá se sigue chequeando cada
    WATCHDOG_CHECK_MINUTES igual que siempre (por eso no hace falta nada
    especial para "la señal silenciosa"), pero no se manda el ⚠️ de nuevo.
    Cuando los datos vuelven, si hubo alerta o ack pendiente se avisa una
    sola vez y se limpian los dos flags."""
    while True:
        time.sleep(WATCHDOG_CHECK_MINUTES * 60)
        if not shared_state.ECOFLOW_READY or not shared_state.USE_PRIVATE_API:
            continue
        is_stale, stale_for_min = mqtt_client._mqtt_staleness()
        try:
            if is_stale and not shared_state._DATA_STALE_ALERTED and not shared_state.STALE_ACK_BY_USER:
                minutos = f"{stale_for_min:.0f}" if stale_for_min is not None else "varios"
                telegram_bot.send_telegram(
                    f"⚠️ No recibo datos de la Delta 2 hace {minutos} min. "
                    "Puede ser un corte de conexión MQTT o que las credenciales vencieron.\n\n"
                    "Si la apagaste vos, mandá /fuiyo y no te vuelvo a avisar hasta que "
                    "vuelva a recibir datos."
                )
                shared_state._DATA_STALE_ALERTED = True
                shared_state._save_persisted_state()
                log.warning("Watchdog: datos viejos hace %s min, alertado", minutos)
            elif not is_stale and (shared_state._DATA_STALE_ALERTED or shared_state.STALE_ACK_BY_USER):
                telegram_bot.send_telegram("✅ Volví a recibir datos de la Delta 2 con normalidad.")
                shared_state._DATA_STALE_ALERTED = False
                shared_state.STALE_ACK_BY_USER = False
                shared_state._save_persisted_state()
                log.info("Watchdog: datos frescos de nuevo, alerta resuelta")
        except Exception:
            log.exception("Error en el watchdog de datos")


# --- Alerta dinámica de proyección: a diferencia del mensaje de /cargas (que
# describe el plan), esto analiza el ritmo de descarga actual y proyecta si la
# batería va a llegar por debajo de la meta del próximo checkpoint del plan.
# Vive acá (y no en dashboard_server.py) porque necesita mandar el aviso por
# Telegram — dashboard_server.py no importa telegram_bot.py a propósito, para
# no crear un ciclo de imports (telegram_bot.py sí importa dashboard_server.py
# para /reporte y /cargas).
PROJECTION_CHECK_MINUTES = 10
PROJECTION_ALERT_MARGIN = 5  # puntos porcentuales por debajo de la meta para disparar la alerta
LOAD_ADVISOR_START_MIN = 6 * 60
LOAD_ADVISOR_END_MIN = 24 * 60  # el timer automatico llega hasta las 12 AM

_projection_alerted_for = {}  # {checkpoint_min: date} último día que ya se avisó ese checkpoint


def _check_battery_projection(now=None) -> None:
    """Un chequeo: proyecta el %SOC en el próximo checkpoint con el ritmo de
    descarga actual. Si va a quedar por debajo de la meta menos el margen y
    bajar carga discrecional ayudaría, avisa por Telegram (una vez por
    checkpoint/día, con rearme si se recupera). Si ni apagando todo se llega
    a la meta (problema de sol, no de consumo), no manda nada — esa info ya
    se ve en vivo en el dashboard/app (threshold_short en el aro)."""
    now = now or datetime.now(TZ)
    minute_of_day = now.hour * 60 + now.minute
    if not (LOAD_ADVISOR_START_MIN <= minute_of_day < LOAD_ADVISOR_END_MIN):
        return
    m = dashboard_server._gather_metrics()
    dashboard_server._log_checkpoint_result(now, m["avg_soc"])
    proj = dashboard_server._project_to_checkpoint(now, m["avg_soc"], m["system_net_w"])
    if proj is None:
        return
    label, floor, projected = proj
    cp_min, _, _ = dashboard_server._next_checkpoint(now)
    today = now.date()
    if m["system_net_w"] >= 0:
        # no está descargando neto ahora mismo: sin riesgo, se puede rearmar
        # la alerta si se había disparado antes y se recuperó
        if _projection_alerted_for.get(cp_min) == today:
            del _projection_alerted_for[cp_min]
        return
    if projected < floor - PROJECTION_ALERT_MARGIN:
        if _projection_alerted_for.get(cp_min) != today:
            # "Mejor caso": cuánto cambiaría la proyección si apagaras TODA la
            # carga discrecional marcada (laptop, ventilador, power bank —
            # nevera/internet quedan afuera porque son protegidas). Si ni así
            # se llega a la meta, decir "bajá carga" es engañoso: el problema
            # no es cuánto estás gastando, es que no hay sol suficiente.
            discretionary_keys = ["laptop"] + dashboard_server.VENTILADOR_DEVICE_KEYS + dashboard_server.POWERBANK_DEVICE_KEYS
            freeable_w = sum(dashboard_server.DEVICE_INFO[k]["watts"] for k in discretionary_keys if shared_state.DEVICE_STATE.get(k))
            best_case_net_w = m["system_net_w"] + freeable_w
            best_proj = dashboard_server._project_to_checkpoint(now, m["avg_soc"], best_case_net_w)
            best_case_helps = best_proj is not None and best_proj[2] >= floor - PROJECTION_ALERT_MARGIN

            if best_case_helps:
                telegram_bot.send_telegram(
                    "⚠️ *Se está yendo de control*\n\n"
                    f"Proyectás *{projected:.0f}%* para {label} (meta {floor}%+)\n"
                    f"Vas en {m['avg_soc']:.1f}%, descargando a {round(m['system_net_w'])} W.\n\n"
                    "Bajá carga (laptop, power bank, ventilador) — la nevera es protegida, no la toques."
                )
                _projection_alerted_for[cp_min] = today
                log.info("Alerta de proyección de batería enviada (checkpoint %s)", label)
            else:
                # Ni apagando todo lo que se puede llegás a la meta: no es
                # problema de consumo, no hay sol suficiente en lo que queda.
                # Ya no se manda nada por Telegram acá — el dashboard/app
                # ahora muestran en vivo la alerta de umbral de batería baja
                # dentro del aro (threshold_short), así que un mensaje aparte
                # con la misma info es redundante (el usuario ya lo tiene a
                # la vista sin necesidad de un push).
                _projection_alerted_for[cp_min] = today
                log.info(
                    "Proyección de batería por debajo de meta sin acción posible (checkpoint %s) — sin aviso, redundante con el dashboard",
                    label,
                )
    elif _projection_alerted_for.get(cp_min) == today:
        del _projection_alerted_for[cp_min]


def battery_projection_timer() -> None:
    while True:
        time.sleep(PROJECTION_CHECK_MINUTES * 60)
        if not shared_state.ECOFLOW_READY:
            continue
        try:
            _check_battery_projection()
        except Exception:
            log.exception("Error en el chequeo de proyección de batería")


def main() -> None:
    try:
        telegram_bot.set_bot_commands()
    except Exception:
        log.exception("No se pudo registrar el menú de comandos (no bloqueante)")
    try:
        telegram_bot.set_dashboard_menu_button()
    except Exception:
        log.exception("No se pudo configurar el botón del dashboard (no bloqueante)")

    threading.Thread(target=telegram_bot.poll_commands, daemon=True).start()
    threading.Thread(target=report_timer, daemon=True).start()
    threading.Thread(target=battery_projection_timer, daemon=True).start()
    threading.Thread(target=ac_check_timer, daemon=True).start()
    threading.Thread(target=watchdog_timer, daemon=True).start()
    threading.Thread(target=quiet_hours_timer, daemon=True).start()
    threading.Thread(target=weekly_cleanup_timer, daemon=True).start()
    threading.Thread(target=dashboard_server.run_dashboard_server, daemon=True).start()
    if shared_state.USE_PRIVATE_API:
        threading.Thread(target=mqtt_client.start_private_mqtt, daemon=True).start()

    log.info(
        "Monitor iniciado. Informe a las :00/:30 (pausado %02d:%02d-%02d:%02d), chequeo AC cada %.1f min.",
        shared_state.QUIET_START_HOUR,
        shared_state.QUIET_START_MINUTE,
        shared_state.QUIET_END_HOUR,
        shared_state.QUIET_END_MINUTE,
        shared_state.AC_CHECK_MINUTES,
    )
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
