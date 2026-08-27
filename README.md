# EcoFlow Monitor

Bot de Telegram + dashboard web que monitorea en tiempo real una EcoFlow Delta 2 (con una batería extra conectada) y manda informes y alertas automáticas. Corre 24/7 como un único proceso en Railway, sin depender de que ninguna PC esté prendida.

## Qué hace

- **Informe bajo demanda** (`/reporte` en Telegram): carga de cada batería, si está cargando o descargando, entrada/salida de energía, si hay corriente conectada, puertos activos, tiempo estimado de autonomía o para llenarse, y tiempo estimado para llegar al 20%.
- **Informe automático**: se manda solo a las :00 y :30 de cada hora, con un horario silencioso configurable (por defecto 23:30–07:00) en el que se pausa para no interrumpir de noche.
- **Alertas en tiempo real**:
  - ⚡ Llegó la corriente / 🔌⚠️ Se fue la luz
  - 🪫 Carga por debajo del umbral configurado (`/alerta <porcentaje>`)
  - 🔋 Carga completa (100%)
  - ⚠️ Datos viejos / conexión MQTT caída (watchdog)
- **Resumen diario**: cuánto entró de solar y cuánto se consumió en el día, 10 minutos antes de que arranque el horario silencioso.
- **Limpieza semanal automática**: borra los mensajes que mandó el bot (no los del usuario, Telegram no lo permite) todos los domingos a las 4 AM.
- **Dashboard web en vivo** (Web App de Telegram + URL pública en Railway): círculo de carga tipo app oficial de EcoFlow, con la hora estimada de autonomía bien visible (lo que a la app oficial le falta), íconos de fuente de energía (corriente/solar/batería extra) con estado de carga/descarga, actualización cada 1 segundo leyendo el caché MQTT (sin golpear la conexión con pedidos activos).

## Arquitectura

Un solo proceso Python (`ecoflow_telegram_monitor.py`), varios threads daemon:

| Thread | Qué hace |
| --- | --- |
| `poll_commands` | Long-polling de comandos de Telegram |
| `report_timer` | Informe automático a las :00/:30 |
| `ac_check_timer` | Chequea AC, batería baja, carga completa, acumula watts para el resumen diario |
| `watchdog_timer` | Avisa si se cae la conexión MQTT |
| `daily_summary_timer` | Resumen diario |
| `quiet_hours_timer` | Avisa al entrar/salir del horario silencioso |
| `weekly_cleanup_timer` | Limpieza semanal de mensajes |
| `start_private_mqtt` | Conexión MQTT persistente a EcoFlow (Private API) |
| `run_dashboard_server` | Servidor HTTP del dashboard web |

### Por qué "Private API" y no la API oficial de developer

La API oficial de EcoFlow (`developer.ecoflow.com`) requiere que el dispositivo esté autorizado en la cuenta developer — un permiso que EcoFlow nunca terminó de habilitar (caso de soporte sin resolver). Se probó exhaustivamente: REST (dos regiones), MQTT oficial, con dos pares de keys distintos — todo falla por el mismo motivo (`8512: no permission to do it`, confirmado tras arreglar un bug de firma que enmascaraba el error real).

Como alternativa se usa la misma API que usa la app móvil de EcoFlow ("Private API", ingeniería inversa del proyecto `hassio-ecoflow-cloud`): login con email/contraseña normales de la cuenta, y comunicación por MQTT (`mqtt-e.ecoflow.com`). El código de la API oficial se dejó en el archivo (funciones `_ecoflow_get`, `_signed_headers`, etc.) por si el día de mañana EcoFlow resuelve el permiso.

## Datos confiables vs. no confiables (aprendido empíricamente)

- `mppt.inWatts` — entrada solar, campo directo, confiable. Confirmado con prueba física (desconectar el panel).
- `inv.acInVol` — voltaje real de entrada AC. Confirmado por la doc oficial de EcoFlow. Se usa para detectar "¿hay corriente?" de forma confiable incluso cuando la batería está llena y el AC está en modo paso-directo (a diferencia de medir watts netos, que da falso negativo en ese caso).
- `mppt.chgType` — tipo de carga activa: `2` = solar, `3` = AC (confirmado por doc oficial), `0` = ninguna.
- `bms_bmsStatus.inputWatts` / `outputWatts` — **rotos** en este equipo (se quedan pegados en 0 pese a que la doc los marca como "campo clave"). Se deriva el neto real restando `pd.wattsOutSum` de `pd.wattsInSum`.
- `pd.remainTime` — **también inconsistente** en este equipo (se queda pegado en un valor viejo). La dirección de carga/descarga se calcula comparando entrada vs. salida en tiempo real en vez de confiar en el signo de este campo.
- `bms_slave.*` — datos de la batería extra, vienen embebidos en la respuesta de la propia Delta 2 (no es un dispositivo separado).

## Comandos de Telegram

| Comando | Qué hace |
| --- | --- |
| `/reporte` | Informe detallado al instante |
| `/alerta <porcentaje>` | Avisar cuando la carga baje de ese nivel (ej: `/alerta 20`) |
| `/start` | Qué hace el bot |
| `/help` | Ver comandos disponibles |

## Variables de entorno

| Variable | Descripción |
| --- | --- |
| `TELEGRAM_BOT_TOKEN` | Token del bot (de @BotFather) |
| `TELEGRAM_CHAT_ID` | Chat ID destino (de @userinfobot) |
| `ECOFLOW_EMAIL` / `ECOFLOW_PASSWORD` | Credenciales normales de la cuenta EcoFlow (Private API) |
| `ECOFLOW_SN_DELTA2` | Número de serie de la Delta 2 |
| `ECOFLOW_ACCESS_KEY` / `ECOFLOW_SECRET_KEY` | Keys de developer.ecoflow.com (sin uso mientras el permiso siga bloqueado) |
| `AC_CHECK_MINUTES` | Cada cuánto chequear AC/batería baja/completa (default: 1) |
| `TZ_NAME` | Zona horaria para horas mostradas (default: `America/Havana`) |
| `QUIET_START_HOUR` / `QUIET_START_MINUTE` | Inicio del horario silencioso (default: 23:30) |
| `QUIET_END_HOUR` / `QUIET_END_MINUTE` | Fin del horario silencioso (default: 07:00) |
| `WEEKLY_CLEANUP_WEEKDAY` / `WEEKLY_CLEANUP_HOUR` | Día/hora de la limpieza semanal (default: domingo 4 AM) |
| `DASHBOARD_URL` | URL pública del dashboard (para el botón "📊 Panel" en Telegram) |
| `PORT` | Puerto del servidor del dashboard (lo define Railway automáticamente) |
| `STATE_FILE` | Ruta del archivo de estado persistido (default: `/data/state.json`) |

## Deploy

Railway, con un volumen montado en `/data` para persistir el estado (umbral de alerta, última vez que llegó corriente, etc.) entre redeploys.

```
railway up --detach
```

El dominio público del dashboard se genera una sola vez con `railway domain -p 8080` (o desde la web de Railway si el CLI no asigna bien el puerto).
