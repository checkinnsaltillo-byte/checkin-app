# Handoff: Integración Breezeway → sesión "Ticket Vision"

> Documento puente. Generado desde la sesión del repo `checkin-app`
> (carpeta "Check in") el 2026-06-11. Pégalo o pide a la otra sesión:
> **"Lee este archivo y guárdalo en memoria"**.

## 1. Objetivo
Recibir alertas en sistemas propios cuando un alojamiento queda **aseado/listo**
para huéspedes, usando la API de Breezeway (membresía de gestión de limpieza).
Falta agregar una **sección "Breezeway"** en la UI de Ticket-vision.

## 2. Lo que YA está implementado (en el repo `checkin-app`, NO en ticket-vision)
- `breezeway.js` — módulo nuevo (Node/Express, ES modules). Contiene:
  - Gestor de token JWT: cachea el access token 24 h, hace refresh, respeta el
    límite de **1 req/min** del endpoint de auth.
  - Receptor de webhook `POST /api/breezeway/webhook` (responde 200 en <10 s,
    procesa async, dedupe por `last_updated`, token anti-spoofing opcional).
  - Lógica de alertas `pushAlert()` → hoy solo log + buffer en memoria.
  - Endpoints: `GET /api/breezeway/alerts`, `GET /api/breezeway/token-test`,
    `POST /api/breezeway/subscribe`.
- `server.js` — importa y registra las rutas (`registerBreezewayRoutes(app)`).
- `.env.example` — documenta las variables nuevas.
- Probado localmente: validación de webhook, task-completed, property-status
  "Ready" y dedupe funcionan.

## 3. Conocimiento de la API Breezeway (verificado en developer.breezeway.io)
- **Base:** `https://api.breezeway.io`
- **Auth:** `POST /public/auth/v1/` con `{client_id, client_secret}`
  → `{access_token, refresh_token}`. Header en requests: `Authorization: JWT <token>`.
- **TTL:** access token 24 h, refresh 30 d. Auth limitada a **1 req/min** (cachear).
- **Refresh:** `POST /public/auth/v1/refresh` con header `Authorization: JWT <refresh_token>`.
- **Suscribir webhook:** `POST /public/webhook/v1/subscribe`
  con `{webhook_type: "property-status" | "task", url}`. La URL debe ser pública
  HTTPS y responder 2XX en <10 s al ping de validación `{"event":"test_webhook_event"}`.
- **Eventos de tarea:** task-created/committed/updated/started/paused/resumed/
  **completed**/etc. Payload: `{event_type, task{...}, last_updated}`. El aseo
  terminado se detecta por `task.finished_at` y `task.home{id,name}`.
- **property-status:** el esquema exacto NO está documentado → capturar el primer
  evento real para confirmar los campos de estado ("ready"/"clean"/etc).

## 4. TAREA PENDIENTE para la sesión Ticket-vision
Agregar una **sección/módulo "Breezeway"** en la UI de Ticket-vision.

Arquitectura de esa UI (repo `checkinnsaltillo-byte/ticket-vision`: `index.html`
+ `styles.css` + `app.js`):
- Nav lateral con `<li class="nav-item" onclick="switchModule('XXX')">`.
- Cada módulo es `<div id="module-XXX" class="hidden">`.
- `switchModule(mod)` (en app.js) muestra/oculta módulos. Módulos actuales:
  `registros`, `tickets`, `huespedes`, `lodgify`.
- Patrón para agregar: nuevo `nav-item` "🧹 Breezeway" → `switchModule('breezeway')`
  + nuevo `<div id="module-breezeway">`, y registrar el módulo en `switchModule`.

⚠️ **OJO arquitectura:** la carpeta `admin/` del repo `checkin-app` es un **espejo
de solo lectura** que el workflow `sync-ticket-vision.yml` sobrescribe cada 6 h
desde el repo `ticket-vision`. El cambio de UI debe hacerse en el repo
**`ticket-vision`**, no en `admin/`.

⚠️ **Cross-origin:** los endpoints `/api/breezeway/*` viven en el server Node de
`checkin-app` (Cloud Run), mientras Ticket-vision es sitio estático + su propio
backend. La sección Breezeway deberá llamar al server Node (definir su URL base y
habilitar CORS si aplica).

## 5. Pendientes operativos / seguridad
1. **Rotar el Client Secret**: se compartió en texto plano el 2026-06-11.
   Pedir a Breezeway regenerarlo y guardarlo solo en gestor de contraseñas + `.env`
   (`BREEZEWAY_CLIENT_ID`, `BREEZEWAY_CLIENT_SECRET`). NO va en el repo.
2. Desplegar el server con URL pública HTTPS → luego llamar a
   `POST /api/breezeway/subscribe` (una vez por `webhook_type`).
3. Conectar `pushAlert()` a un canal real (WhatsApp / correo / hoja de Check in).
4. Confirmar esquema del payload `property-status` con el primer evento real.
