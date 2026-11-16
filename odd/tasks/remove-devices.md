# Remove devices feature

Goal: remove user devices (laptop, ventilador, power bank), their on/charged state,
watts, priority/hierarchy, grouping and the device advisor from backend and app.

Decisions (user, 2026-10-10):
- Keep /cargas plan content (schedule label, goal, checkpoint projection based on
  real in/out net W, weak-charge note); drop device lines and the "apagar todo" emergency.
- Keep the projection alert, without the device "best case" gate and without the
  "bajá carga" device hint.

## Tasks
- [x] 1. Backend: remove devices from shared_state, dashboard_server (advisor, endpoints,
      dashboard HTML/JS), telegram_bot (/on /off /cargado /descargado), monitor projection alert.
      Check: py_compile + grep for leftovers. Done: af44654 (py_compile OK, dashboard JS parses, grep clean).
- [x] 2. App (ecoflow-monitor-app): remove device types, state, API calls, UI section,
      device-only components/icons/utils, README mentions; fix tablet layout.
      Check: npm run typecheck + build:web. Done: ecoflow-monitor-app 7bf9667 (typecheck OK, build:web OK).
