# sky-app — weather/sun panel below the diagram + liquid battery sphere, all real time
Repos: backend `/Users/rid/Proyectos Personales/ecoflow-monitor` (Python, branch feat/sky-weather), app `/Users/rid/Proyectos Personales/ecoflow-monitor-app` (Expo/TS). Designs: `design/sky-background/` in the backend repo (artboards 23 live, 24-33 states, 34 at-risk, 35 ok).
Contract between repos: `/api/status` fields `sky` and `sphere` (see S6).

## Specs
- S1. Weather panel, user: "pon el panel de clima en la parte inferior del diagrama" (design 23). A rounded card below the diagram with the sun or moon moving on its arc, clouds, rain, storm, fog and stars, and a chip with the weather name. User: "el sale y se pone quitalo" — no "Sale / Se pone" text anywhere.
- S2. Background, user: "perfecto ahora vuelve a poenrle el fondo al diagrama que tenia al principio manteniendo los elemtos de ahora" — keep the existing aurora `DiagramBackdrop` (green top, red bottom).
- S3. Liquid battery, user: "ahora ponle la bateria liquida" — an animated liquid sphere replaces the percent ring (artboard 23). User: "ese fondo rojo al final de la esfera puedes quitarlo", "la linea discontinua tambien", "y la linea de mmeta" — no red zone, no dashed 15% line, no meta line, no "15%"/"Meta" chips. User: "es amarillo no naranja" — the yellow is yellow (`#eab308`), never orange.
- S4. Sphere colour rule, user: "dejes toda la esfera en amarillo cuando veas que no se va a cumplir la meta de amanecer y no este cargando de ninguna forma". `sphere.state == "at_risk"` → the whole sphere yellow (artboard 34). `ok` → the whole sphere green (artboard 35). `charging` → yellow up to the target %, green above it (artboard 23); without a target, green.
- S5. Inputs, user: "las entradas son verdes las dos" — AC and solar inputs are both green (already the app's charging colour).
- S6. Real time, user: "todo debe ser en tiempo real". The app polls `/api/status` every second; the sun/moon position follows the device clock; weather comes from `sky` (backend refreshes it every 15 min). Contract: `sky` = `null` or `{"cloud_pct": 0-100, "code": WMO int, "is_day": bool, "sunrise_ts": s, "sunset_ts": s, "updated_ts": s}`; `sphere` = `null` or `{"state": "charging"|"at_risk"|"ok", "dawn_pct": float|null, "reserve_pct": int}`.
- S7. The ring text, including "15% a las X", is not touched (user: "no lo toques dejalo como esta").

## Tasks
- T1 · S6 · route: inline (backend `weather.py`, `sphere` in `dashboard_server.py`, tests, README) · commit: pending · DONE (17 tests green; live Open-Meteo smoke ok)
- T2 · S6 · route: inline (app: status type + props plumbing in `App.tsx`) · commit: pending
- T3 · S3 S4 S5 · route: worker (app: `components/LiquidSphere.tsx`) · commit: pending
- T4 · S1 · route: worker (app: `components/SkyPanel.tsx`, `lib/sky.ts`) · commit: pending
- T5 · S1 S2 S3 S7 · route: inline (app: wire both into `App.tsx`, layout, `npm run typecheck`, web build) · commit: pending
- T6 · all · route: inline (merge both feature branches to master and push; Railway/Vercel deploy) · pending

## Log
- L1. "ya puedes impleemntar y subir todo"
- L2. "todo debe ser en tiempo real"
- L3. "pon el panel de clima en la parte inferior del diagrama"
- L4. "el sale y se pone quitalo"
- L5. "perfecto ahora vuelve a poenrle el fondo al diagrama que tenia al principio manteniendo los elemtos de ahora"
- L6. "ahora ponle la bateria liquida"
- L7. "dejes toda la esfera en amarillo cuando veas que no se va a cumplir la meta de amanecer y no este cargando de ninguna forma ademas ese fondo rojo al final de la esfera puedes quitarlo"
- L8. "la linea discontinua tambien" / "y la linea de mmeta"
- L9. "es amarillo no naranja" / "las entradas son verdes las dos"
