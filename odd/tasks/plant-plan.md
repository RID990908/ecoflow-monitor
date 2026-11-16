# plant-plan — plant suggestion as a night plan (start time + minutes), quiet hours 22:00-07:00
Repos: backend `/Users/rid/Proyectos Personales/ecoflow-monitor` (Python), app `/Users/rid/Proyectos Personales/ecoflow-monitor-app` (Expo/TS). Designs: `design/plant-quiet-hours/` (artboards 01-10 in backend repo).
Contract between repos: `/api/status` gets a new field `plant_plan` (see S7).

## Specs
- S1. Goal, user's words: "quiero saber cuando ponerla en un horario qeu sea prudente y que la carga me de para la noche sin mucho problema quiero hacerlo lo mas minimalista posible y de paso saber el timepo que voy a necesitar para que la carga de la noche entera".
- S2. Quiet hours: user said "no quiero afectar a lso vecinos despues de las 9 pm" and then "primero vamos a bajarlo a las 22:00 para tener mas margen". Quiet hours are 22:00 to 07:00 (local time of the phone). The plant never runs after 22:00; the goal is to reach 07:00 with at least the reserve (15%, `threshold_pct`). UPDATED by L8: the plant can be used 08:00-22:00 and not from 22:01 to 07:59, so the goal is to reach 08:00 with the reserve.
- S3. Start time is anchored one hour before 22:00 when the plant needs at most 1 hour (21:00); if it needs more, it starts earlier by the extra (22:00 minus minutes). The start time must not jump with each reading.
- S4. UI, design `design/plant-quiet-hours/01..10`: a small pill centered at the top, between Entrada and Salida, with plant icon + text. The user: "no quiero que solapes lo que ya esta en el circulo" — the text inside the ring is not touched. The icon over the ring (existing plant icon replacing the green dot) stays as today.
- S5. Pill texts: plan before the start hour: `21:00 · 36 min` (amber). From the start hour on, plant not running: `Ahora · 36 min` (amber); when the slack until 22:00 is under 15 min: same text in red (red icon too). When there is no longer time (minutes needed > time left before 22:00) — user chose "5b": the pill only warns `No da tiempo` (red), proposing nothing. Not needed (reaches 07:00 with margin) — user chose "B": green pill with a check, `Sin planta · 23%` (the % at 07:00). The user said no need to detect whether the plant is on.
- S6. After 22:00 (until 07:00) — user chose "6a": no pill at all and the ring shows the green dot as today. With AC input (`has_ac`) there is no pill and no plant icon, as today.
- S7. Ghost arc on the ring's own % scale: translucent amber segment from the current % to the % the battery will have when the plant stops, no number on it (shown only for plan/ahora/urgent). Contract `plant_plan`: `null` or `{"kind": "plan"|"now"|"urgent"|"no_time"|"not_needed", "start_ts": epoch seconds|null, "minutes": int|null, "target_pct": float|null, "end_pct": float|null}`; `end_pct` is the % at 07:00 without plant (for `not_needed`). Minutes rounded up to 5. Backend computes with the client clock offset (`_client_now(utc_offset_min)`). Plant power configurable (`PLANT_POWER_W`, default 900, example value — the user has not given the real one). Load = current output watts; the plan is only computed from 17:00 local.
- S8. The interim criterion in `lib/plantNeed.ts` (reserve reached before 23:00 or before 07:00) is replaced by `plant_plan`.
- S9. Fixed target, UI (user L9-L10): the yellow is a fixed segment from the start of the ring to the target % ("con x porciento llegas a la mañana sin problemas"), drawn under the green bar, so it only shows when the green drops below it; top panel as before (`21:00 · 35 min`, `Ahora · N min`, ...); `No alcanza` (not `Solo hasta`) when even a full battery cannot make it. The target must be REAL (L11 "calcula al meta para ser real"): it uses the night consumption learned from the user's own history (average output W per hour of day, learned in the background and persisted), not the instantaneous output; fallback to the current output until enough data exists.

## Tasks
- T1 · S2 S3 S5 S6 S7 · route: worker (backend, `dashboard_server.py`, `shared_state.py`) · commit: 0740e77 (backend, feat/plant-plan) · DONE
- T2 · S4 S5 S6 S7 S8 · route: inline (app `types.ts`, `lib/plantNeed.ts`, `components/PlantPill.tsx`, `App.tsx`, `components/LiveDot.tsx`, ring ghost arc) · commit: 202a68a (app, feat/plant-plan) · DONE
- T4 · S9 · route: worker (backend night-consumption learning + sampler) · commit: pending
- T5 · S9 · route: inline (app: fixed ghost under green; fixed target) · commit: pending
- T3 · all · route: inline (typecheck, backend py_compile, unit check of plan logic, preview) · PARTIAL: static checks done; on-device preview pending backend deploy

## Log
- L1. "vamos a cambair en la app el criterio de usar la planta, si ves que para el final del dia el 15% no se va a dar puedes mostrar el icono ejemplo el 15% lo va a dar a las 11 pm entonces ahi se pone, ejemplo 2 si el 15% lo va a dar antes de las 7 de la mañana tambien es un criterio a tener en cuenta"
- L2. "necesito repenzar este tema de la planta ya que tambien pasa qeu no quiero afectar a lso vecinos despues de las 9 pm entonces dame 10 ideas con visual para poder ver que propones"
- L3. "pero integralo al diseño actual eso no va a cambiar"
- L4. "no quiero que solapes lo que ya esta en el circulo"
- L5. "primero vamos a bajarlo a las 22:00 para tener mas margen y lo otro como se sabra si la planta es la que esta encendida o no o sea eso no hace falta tu muestrame todas las variantes que me planteaste y yo te digo"
- L6. "la b mme parece bien ahora muestrame todos los flujos para revisarlos"
- L7. "5b 6a"
- L8. "contempla desde las 8 am hasta las 22:00 como timepo en que se puede usar y de las 22:01 hasta las 7:59 como tiempo que no se puede usar"
- L9. "lo que quiero es que me digas con x porciento llegas a la mañana sin problemas fijes ahi el amarillo y cuando la barra verde vaya bajando se muestre como estaba la parde de arriba estaba bien antes"
- L10. "pon no alcanza y repienza el tema de la circunferencai amarilla"
- L11. "calcula al meta para ser real"
