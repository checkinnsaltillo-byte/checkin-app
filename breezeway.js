// =====================================================================
//  Integración Breezeway (prueba) — alertas de aseo / estado de alojamientos
// ---------------------------------------------------------------------
//  Qué hace:
//   1. Maneja la autenticación (token JWT con TTL de 24 h y refresh de 30 d),
//      respetando el límite de 1 req/min del endpoint de auth.
//   2. Expone un endpoint receptor de webhooks que Breezeway llama cuando
//      cambia el estado de una propiedad o se completa una tarea (aseo).
//   3. Genera "alertas" (por ahora: log + buffer en memoria consultable) que
//      luego puedes redirigir a WhatsApp / correo / la hoja de Check in.
//   4. Endpoints de administración para suscribir / listar / borrar webhooks.
//
//  Docs: https://developer.breezeway.io
//  Auth:   POST https://api.breezeway.io/public/auth/v1/        { client_id, client_secret }
//          -> { access_token, refresh_token }   (header: Authorization: JWT <token>)
//  Webhook: POST https://api.breezeway.io/public/webhook/v1/subscribe
//           { webhook_type: "property-status" | "task", url }
// =====================================================================

const BREEZEWAY_API_BASE = "https://api.breezeway.io";
const AUTH_PATH = "/public/auth/v1/";
const REFRESH_PATH = "/public/auth/v1/refresh";
const SUBSCRIBE_PATH = "/public/webhook/v1/subscribe";

// El access token dura 24 h; lo renovamos a las 23 h por seguridad.
const ACCESS_TTL_MS = 23 * 60 * 60 * 1000;
// El endpoint de auth está limitado a 1 req/min. No reintentamos antes de eso.
const AUTH_MIN_INTERVAL_MS = 60 * 1000;

// --------------------------- Estado en memoria ---------------------------
const tokenState = {
  accessToken: null,
  refreshToken: null,
  fetchedAt: 0,        // timestamp (ms) de cuándo obtuvimos el access token
  lastAuthAttempt: 0,  // para respetar el rate-limit de 1 req/min
};

// Buffer circular de alertas recientes (consultable vía GET /api/breezeway/alerts)
const recentAlerts = [];
const MAX_ALERTS = 100;

// Dedupe por last_updated (Breezeway puede reenviar el mismo estado)
const seenUpdates = new Map(); // key -> last_updated
const MAX_SEEN = 500;

// --------------------------- Helpers de credenciales ---------------------------
function getCredentials() {
  const client_id = process.env.BREEZEWAY_CLIENT_ID;
  const client_secret = process.env.BREEZEWAY_CLIENT_SECRET;
  if (!client_id || !client_secret) {
    throw new Error(
      "Faltan BREEZEWAY_CLIENT_ID / BREEZEWAY_CLIENT_SECRET en el entorno (.env)."
    );
  }
  return { client_id, client_secret };
}

// --------------------------- Autenticación ---------------------------
// Mutex: si varias requests concurrentes piden token al mismo tiempo (común
// en cold-start del Cloud Run con un pool de 6+ workers), TODAS comparten
// la misma promesa en vuelo — solo 1 POST a Breezeway, respeta el rate
// limit de 1 req/min sin estrangular las requests legítimas.
let _authInFlight = null;

async function authenticate() {
  if (_authInFlight) return _authInFlight;
  _authInFlight = (async () => {
    try {
      const sinceLast = Date.now() - tokenState.lastAuthAttempt;
      if (sinceLast < AUTH_MIN_INTERVAL_MS) {
        const waitS = Math.ceil((AUTH_MIN_INTERVAL_MS - sinceLast) / 1000);
        throw new Error(
          `Auth de Breezeway limitada a 1 req/min. Espera ~${waitS}s antes de reintentar.`
        );
      }
      tokenState.lastAuthAttempt = Date.now();

      const { client_id, client_secret } = getCredentials();
      const res = await fetch(`${BREEZEWAY_API_BASE}${AUTH_PATH}`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({ client_id, client_secret }),
      });

      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(
          `Auth Breezeway falló (${res.status}): ${data?.message || JSON.stringify(data)}`
        );
      }

      tokenState.accessToken = data.access_token;
      tokenState.refreshToken = data.refresh_token;
      tokenState.fetchedAt = Date.now();
      console.log("🔑 Breezeway: nuevo access token obtenido (válido ~24 h).");
      return tokenState.accessToken;
    } finally {
      // Limpiamos el mutex tras un breve delay para que requests inmediatas
      // posteriores tomen el token recién cacheado, no inicien otra auth.
      setTimeout(() => { _authInFlight = null; }, 50);
    }
  })();
  return _authInFlight;
}

async function refreshAccessToken() {
  if (!tokenState.refreshToken) return authenticate();

  const sinceLast = Date.now() - tokenState.lastAuthAttempt;
  if (sinceLast < AUTH_MIN_INTERVAL_MS) {
    const waitS = Math.ceil((AUTH_MIN_INTERVAL_MS - sinceLast) / 1000);
    throw new Error(`Refresh limitado a 1 req/min. Espera ~${waitS}s.`);
  }
  tokenState.lastAuthAttempt = Date.now();

  const res = await fetch(`${BREEZEWAY_API_BASE}${REFRESH_PATH}`, {
    method: "POST",
    headers: {
      Authorization: `JWT ${tokenState.refreshToken}`,
      Accept: "application/json",
    },
  });

  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    // Si el refresh expiró (30 d) o es inválido, re-autenticamos desde cero.
    console.warn(`⚠️ Breezeway refresh falló (${res.status}); re-autenticando.`);
    tokenState.refreshToken = null;
    return authenticate();
  }

  tokenState.accessToken = data.access_token;
  tokenState.refreshToken = data.refresh_token;
  tokenState.fetchedAt = Date.now();
  console.log("🔄 Breezeway: access token renovado vía refresh.");
  return tokenState.accessToken;
}

async function getAccessToken({ forceRefresh = false } = {}) {
  const expired = Date.now() - tokenState.fetchedAt > ACCESS_TTL_MS;
  if (!forceRefresh && tokenState.accessToken && !expired) {
    return tokenState.accessToken;
  }
  if (tokenState.accessToken && (expired || forceRefresh)) {
    return refreshAccessToken();
  }
  return authenticate();
}

// Fetch autenticado contra la API de Breezeway. Reintenta una vez ante 401.
async function breezewayFetch(pathname, options = {}, _retried = false) {
  const token = await getAccessToken();
  const res = await fetch(`${BREEZEWAY_API_BASE}${pathname}`, {
    ...options,
    headers: {
      Authorization: `JWT ${token}`,
      Accept: "application/json",
      ...(options.body ? { "Content-Type": "application/json" } : {}),
      ...(options.headers || {}),
    },
  });

  if (res.status === 401 && !_retried) {
    await getAccessToken({ forceRefresh: true });
    return breezewayFetch(pathname, options, true);
  }
  return res;
}

// --------------------------- Lógica de alertas ---------------------------
function pushAlert(alert) {
  const enriched = { ...alert, received_at: new Date().toISOString() };
  recentAlerts.unshift(enriched);
  if (recentAlerts.length > MAX_ALERTS) recentAlerts.length = MAX_ALERTS;

  console.log(`🔔 ALERTA Breezeway [${alert.kind}] ${alert.title} — ${alert.detail}`);

  // Persistir al Sheet (fire-and-forget — no bloquea la respuesta del webhook).
  // Sin esto, el buffer en memoria se pierde con cada cold-start del Cloud Run.
  persistAlertToSheet(enriched).catch(err =>
    console.warn("⚠️ Persist alert to sheet falló:", err?.message || err)
  );

  // TODO (siguiente paso): redirigir aquí a WhatsApp / correo / hoja de Check in.
  //   - WhatsApp: notifyWhatsApp(enriched)
  //   - Correo:   sendMail(enriched)
  //   - Hoja:     POST al CHECKIN_WEB_APP_URL con action: "breezeway_alert"
  return enriched;
}

// Evita procesar dos veces el mismo cambio (Breezeway reenvía estado completo).
function isDuplicate(key, lastUpdated) {
  if (!key || !lastUpdated) return false;
  if (seenUpdates.get(key) === lastUpdated) return true;
  seenUpdates.set(key, lastUpdated);
  if (seenUpdates.size > MAX_SEEN) {
    // Limpieza simple: borra la entrada más antigua.
    const firstKey = seenUpdates.keys().next().value;
    seenUpdates.delete(firstKey);
  }
  return false;
}

async function fetchTaskById(taskId) {
  // Trae el task COMPLETO desde la API de BZW. El webhook a veces llega
  // con datos parciales (sin scheduled_date / due_date) — esto enriquece.
  if (!taskId) return null;
  try {
    const r = await breezewayFetch(`/public/inventory/v1/task/${taskId}`);
    if (!r.ok) return null;
    return await r.json();
  } catch (e) {
    console.warn(`[BZW] fetchTaskById ${taskId} falló:`, e?.message || e);
    return null;
  }
}

async function handleTaskEvent(payload) {
  const { event_type, last_updated } = payload;
  const webhookTask = payload.task || {};
  // ENRICH: si el webhook trae solo id+name+status, traer el task completo
  // de la API. Esto puebla scheduled_date, due_date, finished_at, etc.
  const full = await fetchTaskById(webhookTask.id);
  // Merge: full prevalece (más completo), pero conserva campos del webhook
  // si la API no los devuelve.
  const task = { ...webhookTask, ...(full || {}) };
  const home = task.home || (full?.home_id ? { id: full.home_id, name: full.home_name } : {});
  const key = `task:${task.id}`;
  if (isDuplicate(key, last_updated)) return null;

  // Nos interesa principalmente cuando el aseo queda terminado.
  const isCompleted = event_type === "task-completed" || Boolean(task.finished_at);
  if (!isCompleted) {
    // Otros eventos de tarea: los registramos con TODOS los campos disponibles
    // para que aparezcan en el sheet (scheduled_date, due_date, etc.).
    return pushAlert({
      kind: "task",
      event_type,
      title: `Tarea: ${task.name || task.id}`,
      detail: `${home.name || task.home_name || "—"} · estado: ${task.status?.name || task.status?.code || task.status || "?"}`,
      property: { id: home.id || task.home_id, name: home.name || task.home_name },
      task: {
        id: task.id,
        name: task.name,
        type: task.type_department || task.type?.name,
        status: task.status?.name || task.status,
        scheduled_date: task.scheduled_date || "",
        due_date: task.due_date || task.task_date || "",
        finished_at: task.finished_at || "",
        finished_by: task.finished_by?.name || task.finished_by || "",
        report_url: task.report_url || "",
      },
      raw: { ...payload, task },
    });
  }

  // Filtro opcional por tipo de tarea (p.ej. "clean") configurable por env.
  const typeFilter = (process.env.BREEZEWAY_CLEANING_TYPE_FILTER || "").toLowerCase();
  const taskTypeName = String(task.type?.name || task.name || "").toLowerCase();
  if (typeFilter && !taskTypeName.includes(typeFilter)) {
    return null; // No es un aseo según el filtro; lo ignoramos.
  }

  return pushAlert({
    kind: "task-completed",
    event_type,
    title: `✅ Aseo terminado: ${home.name || task.home_name || "Alojamiento " + (home.id || task.home_id)}`,
    detail: `Tarea "${task.name || ""}" finalizada${task.finished_at ? " a las " + task.finished_at : ""}`,
    property: { id: home.id || task.home_id, name: home.name || task.home_name },
    task: {
      id: task.id,
      name: task.name,
      type: task.type_department || task.type?.name,
      scheduled_date: task.scheduled_date || "",
      due_date: task.due_date || task.task_date || "",
      finished_at: task.finished_at,
      finished_by: task.finished_by?.name || task.finished_by || "",
      status: task.status?.name || task.status,
      report_url: task.report_url || "",
    },
    raw: { ...payload, task },
  });
}

function handlePropertyStatusEvent(payload) {
  // El esquema exacto no está documentado; extraemos de forma defensiva y
  // guardamos el payload completo para inspeccionarlo en la primera prueba real.
  const prop = payload.property || payload.home || payload;
  const status = payload.status || prop.status || {};
  const statusName = String(
    status.name || status.code || payload.property_status || ""
  ).toLowerCase();
  const key = `property:${prop.id || prop.external_id}`;
  if (isDuplicate(key, payload.last_updated)) return null;

  // Heurística: "ready"/"clean"/"listo" => alojamiento listo para huéspedes.
  const isReady = /ready|clean|listo|limpio/.test(statusName);

  return pushAlert({
    kind: isReady ? "property-ready" : "property-status",
    event_type: payload.event_type || "property-status",
    title: isReady
      ? `🏠✨ Alojamiento listo: ${prop.name || prop.id}`
      : `🏠 Cambio de estado: ${prop.name || prop.id}`,
    detail: `Estado: ${status.name || status.code || payload.property_status || "?"}`,
    property: { id: prop.id, name: prop.name, external_id: prop.external_id },
    status: status.name || status.code || payload.property_status,
    raw: payload,
  });
}

// --------------------------- Persistencia a Sheet ---------------------------
// El buffer en memoria (recentAlerts) se pierde en cada cold-start o cuando
// Cloud Run escala a nuevas instancias. Para sobrevivir, escribimos cada
// alerta a una hoja "Breezeway_Alerts" via el Apps Script CHECKIN_WEB_APP_URL
// y leemos desde ahí en /api/breezeway/alerts.

/** Colapsa el estado raw de BZW a 3 etiquetas humanas:
 *  "Terminado" | "En proceso" | "Pendiente".
 *  Lógica (gana la más definitiva):
 *   - finished_at presente → Terminado
 *   - started_at o status in_progress/paused → En proceso
 *   - cualquier otra cosa → Pendiente */
function resolveStatusLabel(t, r) {
  if (t?.finished_at || r?.finished_at) return "Terminado";
  const rawStatus = String(
    t?.status?.name || t?.status ||
    r?.type_task_status?.code || r?.type_task_status?.name ||
    r?.status?.name || r?.status || ""
  ).toLowerCase().replace(/[\s_-]+/g, "");
  if (/finish|complet|done/.test(rawStatus)) return "Terminado";
  const hasStarted = !!(t?.started_at || r?.started_at);
  if (hasStarted || /inprogress|progress|started|running|active|paus/.test(rawStatus)) {
    return "En proceso";
  }
  return "Pendiente";
}

function flatAlertForSheet(a) {
  // Aplana la alerta a las columnas de la hoja Breezeway_Alerts.
  //
  // ─── Glosario de fechas REALES (schema verificado contra API BZW) ───
  // scheduled_date  : Fecha en que la task DEBE ejecutarse (la fecha que se
  //                   ve en BZW UI como "📅 jun. 15, 2026" — BZW NO tiene un
  //                   campo "due_date" separado; "fecha límite" en su UI
  //                   ES scheduled_date).
  // scheduled_time  : Hora prevista (puede ser null si no se especificó).
  // started_at      : Cuándo el operario picó "Iniciar".
  // finished_at     : Cuándo se marcó como completada.
  // created_at      : Cuándo se generó la task en BZW.
  // updated_at      : Última modificación en BZW.
  // received_at     : Reloj local de Cloud Run al recibir el webhook.
  // arrival_date    : Check-in de la reservación Lodgify ligada.
  // departure_date  : Check-out de la reservación Lodgify ligada.
  const t = a.task || {};
  const r = a.raw || {};
  const rt = r.task || {};
  // Helpers para colapsar a string sin importar si el campo es objeto/string
  const str = (v) => (v == null) ? "" : (typeof v === "object" ? (v.name || v.code || JSON.stringify(v)) : String(v));
  const assignNames = Array.isArray(r.assignments)
    ? Array.from(new Set(r.assignments.map(x => x?.full_name || x?.name).filter(Boolean))).join(", ")
    : "";
  const tagsArr = [...(Array.isArray(r.tags) ? r.tags : []), ...(Array.isArray(r.task_tags) ? r.task_tags : [])]
    .map(x => typeof x === "object" ? (x?.name || x?.label) : x).filter(Boolean);
  return {
    event_type:    a.event_type || "",
    kind:          a.kind || "",
    task_id:       t.id ?? r.id ?? "",
    task_name:     t.name ?? r.name ?? "",
    task_type:     str(t.type || r.type_department),
    // ─── Fechas ───
    scheduled_date: t.scheduled_date ?? r.scheduled_date ?? rt.scheduled_date ?? "",
    scheduled_time: r.scheduled_time ?? rt.scheduled_time ?? "",
    started_at:    r.started_at ?? t.started_at ?? rt.started_at ?? "",
    finished_at:   t.finished_at ?? r.finished_at ?? rt.finished_at ?? "",
    created_at:    r.created_at ?? t.created_at ?? rt.created_at ?? "",
    updated_at:    r.updated_at ?? t.updated_at ?? rt.updated_at ?? "",
    arrival_date:  a._arrival_date ?? "",
    departure_date: a._departure_date ?? "",
    // ─── Personas / asignación ───
    finished_by:   str(t.finished_by),
    assigned_to:   assignNames,
    // ─── Propiedad ───
    home_id:       a.property?.id ?? r.home_id ?? "",
    property_name: a.property?.name ?? "",
    // ─── Vinculación a reserva ───
    lodgify_id:    r.linked_reservation?.external_reservation_id
                || r.linked_reservation?.external_id
                || "",
    // ─── Detalle de la task (schema BZW real) ───
    priority:      r.type_priority ?? "",
    // status: BZW lo entrega como objeto type_task_status:{code,name,stage}
    status:        str(r.type_task_status || t.status || r.status),
    // status_label: colapsado a 3 etiquetas humanas para mostrar en UI
    status_label:  resolveStatusLabel(t, r),
    description:   r.description ?? t.description ?? "",
    summary:       r.summary ?? "",
    total_time:    r.total_time ?? "",       // tiempo real ejecutado (BZW)
    paused:        r.paused === true ? "true" : (r.paused === false ? "false" : ""),
    tags:          tagsArr.join(", "),
    rate_type:     r.rate_type ?? "",
    rate_paid:     r.rate_paid ?? "",
    template_id:   r.template_id ?? t.template_id ?? "",
    report_url:    t.report_url ?? r.report_url ?? "",
    detail:        a.detail ?? a.title ?? "",
    raw_json:      JSON.stringify(a.raw || a),
  };
}

async function persistAlertToSheet(alert) {
  const url = process.env.CHECKIN_WEB_APP_URL;
  if (!url) {
    console.warn("CHECKIN_WEB_APP_URL no configurado — alerta NO persistida.");
    return;
  }
  // El Apps Script lee el body como text/plain (sin preflight CORS) y los
  // campos van al primer nivel del JSON junto con la action — convención del
  // resto del backend (ver server.js → update_facturapi_folio_strict).
  const payload = { action: "breezeway_alert", ...flatAlertForSheet(alert) };
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "text/plain;charset=utf-8" },
    body: JSON.stringify(payload),
    redirect: "follow",
  });
  if (!res.ok) {
    throw new Error(`Apps Script HTTP ${res.status}`);
  }
  const txt = await res.text();
  // Apps Script puede devolver HTML de error si el deployment está mal; si
  // no es JSON, lanzamos el preview en el log.
  try { const j = JSON.parse(txt); if (j && j.ok === false) throw new Error(j.error || "save failed"); }
  catch (e) {
    if (txt.startsWith("<")) throw new Error("Apps Script devolvió HTML (¿deployment con auth?)");
    throw e;
  }
}

async function fetchAlertsFromSheet(limit = 100) {
  const url = process.env.CHECKIN_WEB_APP_URL;
  if (!url) return null;
  const sep = url.includes("?") ? "&" : "?";
  const res = await fetch(
    `${url}${sep}action=breezeway_alerts_list&limit=${limit}`,
    { method: "GET", redirect: "follow" }
  );
  if (!res.ok) throw new Error(`Apps Script HTTP ${res.status}`);
  const json = await res.json().catch(() => ({}));
  return Array.isArray(json.alerts) ? json.alerts : [];
}

// --------------------------- Cache de propiedades ---------------------------
const PROPERTIES_TTL_MS = 10 * 60 * 1000; // 10 min
let propertiesCache = { fetchedAt: 0, list: [] };

async function fetchPropertiesCached() {
  if (Date.now() - propertiesCache.fetchedAt < PROPERTIES_TTL_MS && propertiesCache.list.length) {
    return propertiesCache.list;
  }
  const all = [];
  for (let page = 1; page <= 20; page++) {
    const params = new URLSearchParams({ status: "active", limit: "100", page: String(page) });
    const apiRes = await breezewayFetch(
      `/public/inventory/v1/property?${params.toString()}`,
      { method: "GET" }
    );
    if (!apiRes.ok) break;
    const data = await apiRes.json().catch(() => ({}));
    const items = Array.isArray(data.results) ? data.results : Array.isArray(data) ? data : [];
    all.push(...items);
    if (!data.next || items.length < 100) break;
  }
  propertiesCache = { fetchedAt: Date.now(), list: all };
  return all;
}

// --------------------------- Registro de rutas Express ---------------------------
export function registerBreezewayRoutes(app) {
  // ---- Webhook receptor (lo que Breezeway llama) ----
  // Para que la suscripción valide la URL, debe responder 2XX en <10 s.
  app.post("/api/breezeway/webhook", (req, res) => {
    const body = req.body || {};

    // 1) Token opcional anti-spoofing: si configuras BREEZEWAY_WEBHOOK_TOKEN,
    //    la URL suscrita debe incluir ?token=... (lo añade /subscribe abajo).
    const expectedToken = process.env.BREEZEWAY_WEBHOOK_TOKEN;
    if (expectedToken && req.query?.token !== expectedToken) {
      return res.status(401).json({ ok: false, message: "Token inválido." });
    }

    // 2) Ping de validación de Breezeway al suscribir.
    if (body.event === "test_webhook_event") {
      console.log("✅ Breezeway: webhook de validación recibido.");
      return res.status(200).json({ ok: true, validated: true });
    }

    // 3) Respondemos 200 de inmediato y procesamos sin bloquear (regla <10 s).
    res.status(200).json({ ok: true });

    try {
      if (body.task || /^task/.test(body.event_type || "")) {
        // handleTaskEvent ahora es async (enriquece con API). Fire-and-forget.
        handleTaskEvent(body).catch(err =>
          console.error("❌ Error procesando webhook task:", err?.message || err));
      } else {
        handlePropertyStatusEvent(body);
      }
    } catch (err) {
      console.error("❌ Error procesando webhook Breezeway:", err?.message || err);
    }
  });

  // ---- Consultar alertas recientes (Sheet-backed, sobrevive cold starts) ----
  app.get("/api/breezeway/alerts", async (req, res) => {
    const limit = Math.min(Number(req.query?.limit) || 100, 5000);
    try {
      // 1) Lee del Sheet (fuente de verdad persistente).
      const fromSheet = await fetchAlertsFromSheet(limit);
      if (fromSheet) {
        // Re-hidrata cada fila plana al formato que la UI espera (task, property, raw…).
        const reshaped = fromSheet.map(r => ({
          id: r.id,
          received_at: r.received_at,
          event_type: r.event_type,
          kind: r.kind,
          title: r.detail || "",
          detail: r.detail || "",
          last_updated: r.finished_at || r.scheduled_date || r.due_date || r.received_at,
          property: { id: r.home_id, name: r.property_name },
          task: {
            id: r.task_id,
            name: r.task_name,
            type: r.task_type,
            scheduled_date: r.scheduled_date,
            scheduled_time: r.scheduled_time || "",
            started_at: r.started_at || "",
            finished_at: r.finished_at,
            finished_by: r.finished_by,
            status: r.status,
            status_label: r.status_label || "",
            description: r.description || "",
            summary: r.summary || "",
            total_time: r.total_time || "",
            paused: r.paused || "",
            tags: r.tags || "",
            rate_type: r.rate_type || "",
            rate_paid: r.rate_paid || "",
            template_id: r.template_id || "",
            report_url: r.report_url || "",
            assigned_to: r.assigned_to || "",
            // Si el sheet trae las nuevas columnas, las pasamos para que
            // el frontend pueda mostrarlas sin volver a buscar en LG_STATE.
            created_at: r.created_at || undefined,
            updated_at: r.updated_at || undefined,
            arrival_date: r.arrival_date || undefined,
            departure_date: r.departure_date || undefined,
          },
          raw: r.raw || (() => { try { return JSON.parse(r.raw_json || "{}"); } catch (_) { return {}; } })(),
          // Campo extra para que la UI sepa que vino del sheet
          _persisted: true,
        }));
        return res.json({ ok: true, count: reshaped.length, source: "sheet", alerts: reshaped });
      }
      // 2) Fallback: buffer en memoria (solo si la app script no responde)
      res.json({ ok: true, count: recentAlerts.length, source: "memory-fallback", alerts: recentAlerts.slice(0, limit) });
    } catch (err) {
      console.warn("⚠️ /alerts read from sheet falló, usando buffer:", err?.message || err);
      res.json({ ok: true, count: recentAlerts.length, source: "memory-fallback", alerts: recentAlerts.slice(0, limit) });
    }
  });

  // ---- Probar la autenticación (debug) ----
  app.get("/api/breezeway/token-test", async (_req, res) => {
    try {
      const token = await getAccessToken();
      res.json({
        ok: true,
        hasToken: Boolean(token),
        tokenPreview: token ? `${token.slice(0, 12)}…` : null,
        fetchedAt: new Date(tokenState.fetchedAt).toISOString(),
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: String(err?.message || err) });
    }
  });

  // ---- Crear suscripción de webhook ----
  // body: { webhook_type: "property-status" | "task", url? }
  // Si no mandas url, usa BREEZEWAY_WEBHOOK_PUBLIC_URL (+ token).
  app.post("/api/breezeway/subscribe", async (req, res) => {
    try {
      const webhook_type = req.body?.webhook_type || "property-status";
      let url = req.body?.url || process.env.BREEZEWAY_WEBHOOK_PUBLIC_URL;
      if (!url) {
        return res.status(400).json({
          ok: false,
          message:
            "Falta 'url' o BREEZEWAY_WEBHOOK_PUBLIC_URL (debe ser pública y HTTPS).",
        });
      }
      // Añade el token anti-spoofing a la URL suscrita si está configurado.
      const token = process.env.BREEZEWAY_WEBHOOK_TOKEN;
      if (token && !/[?&]token=/.test(url)) {
        url += (url.includes("?") ? "&" : "?") + "token=" + encodeURIComponent(token);
      }

      const apiRes = await breezewayFetch(SUBSCRIBE_PATH, {
        method: "POST",
        body: JSON.stringify({ webhook_type, url }),
      });
      const data = await apiRes.json().catch(() => ({}));
      // "Same webhook config already exists" (422) NO es realmente un error
      // desde la perspectiva del usuario — significa que la suscripción ya
      // existe con esa misma URL + token. Devolvemos OK con un flag.
      const isAlreadySubscribed =
        apiRes.status === 422 &&
        /already exists/i.test(String(data?.description || data?.error || ""));
      if (isAlreadySubscribed) {
        return res.json({
          ok: true,
          already_subscribed: true,
          subscribed: { webhook_type, url },
          breezeway: data,
        });
      }
      if (!apiRes.ok) {
        return res.status(apiRes.status).json({ ok: false, breezeway: data });
      }
      res.json({ ok: true, subscribed: { webhook_type, url }, breezeway: data });
    } catch (err) {
      res.status(500).json({ ok: false, error: String(err?.message || err) });
    }
  });

  // ---- Lista de propiedades de Breezeway (paginada, agregada) ----
  app.get("/api/breezeway/properties", async (req, res) => {
    try {
      const q = req.query || {};
      const status = String(q.status || "active");
      const maxPages = Math.min(Number(q.max_pages) || 20, 50);
      const limit = Math.min(Number(q.limit) || 100, 100);

      const all = [];
      const pagesMeta = [];
      for (let page = 1; page <= maxPages; page++) {
        const params = new URLSearchParams({
          status,
          limit: String(limit),
          page: String(page),
        });
        const apiRes = await breezewayFetch(
          `/public/inventory/v1/property?${params.toString()}`,
          { method: "GET" }
        );
        const data = await apiRes.json().catch(() => ({}));
        if (!apiRes.ok) {
          return res.status(apiRes.status).json({ ok: false, breezeway: data, page });
        }
        const items = Array.isArray(data.results)
          ? data.results
          : Array.isArray(data) ? data : [];
        pagesMeta.push({ page, fetched: items.length, has_next: Boolean(data.next) });
        all.push(...items);
        if (!data.next || items.length < limit) break;
      }
      res.json({ ok: true, count: all.length, pages: pagesMeta, properties: all });
    } catch (err) {
      res.status(500).json({ ok: false, error: String(err?.message || err) });
    }
  });

  // ---- HISTÓRICO de tareas (consulta Breezeway por propiedad, agrega todo) ----
  // Breezeway exige home_id O reference_property_id por consulta. Estrategia:
  //   1) Lista propiedades activas (cacheada en memoria 10 min).
  //   2) Para cada propiedad → consulta /public/inventory/v1/task/ con el
  //      filtro de fechas; agrega todos los resultados.
  //   3) Concurrencia limitada (default 6) para no saturar la API.
  //
  // Query params:
  //   from           ISO YYYY-MM-DD     (default: hace 30 días)
  //   to             ISO YYYY-MM-DD     (default: hoy + 1)
  //   key            "finished_at" | "scheduled_date" | "created_at" | "updated_at"
  //                  (default: finished_at = aseos terminados)
  //   type_department "housekeeping" | "maintenance" | "inspection" | "safety"
  //                  (default: housekeeping para aseo)
  //   home_id        si se pasa, salta el listado de propiedades y consulta solo esa
  //   max_pages      por propiedad (default 5)
  //   limit          por página (default 100)
  //   concurrency    propiedades en paralelo (default 6, máx 12)
  app.get("/api/breezeway/tasks", async (req, res) => {
    try {
      const q = req.query || {};
      const today = new Date();
      const defFrom = new Date(today.getTime() - 365 * 86400000);
      const defTo   = new Date(today.getTime() + 86400000);
      const fromIso = String(q.from || defFrom.toISOString().slice(0, 10));
      const toIso   = String(q.to   || defTo.toISOString().slice(0, 10));
      const key     = String(q.key || "finished_at");
      // Por default, "any" → no filtramos por departamento. Antes era
      // "housekeeping" por defecto, lo cual ocultaba mantenimiento/inspección.
      const typeDept = q.type_department != null ? String(q.type_department) : "";
      const homeIdFilter = q.home_id ? String(q.home_id) : "";
      const maxPagesPerHome = Math.min(Number(q.max_pages) || 5, 20);
      const limit = Math.min(Number(q.limit) || 100, 100);
      const concurrency = Math.max(1, Math.min(Number(q.concurrency) || 6, 12));

      // 1) Lista de propiedades + índice id → name (para enriquecer las tasks).
      const propsList = await fetchPropertiesCached();
      const propIdx = new Map();
      for (const p of propsList) propIdx.set(p.id, p);
      const homes = homeIdFilter
        ? [{ id: Number(homeIdFilter), name: propIdx.get(Number(homeIdFilter))?.name }]
        : propsList;
      if (!homes.length) {
        return res.json({
          ok: true, count: 0, from: fromIso, to: toIso,
          message: "Sin propiedades activas en Breezeway.",
          tasks: [],
        });
      }

      // 2) Para cada propiedad → consulta tasks (con paginación interna).
      // Breezeway: filtro de rango = "YYYY-MM-DD,YYYY-MM-DD" en un solo param.
      // Respuesta DRF: { limit, page, results, total_pages, total_results }.
      const dateFilterValue = `${fromIso},${toIso}`;
      async function fetchHomeTasks(homeId) {
        const out = [];
        for (let page = 1; page <= maxPagesPerHome; page++) {
          const params = new URLSearchParams({
            home_id: String(homeId),
            [key]: dateFilterValue,
            limit: String(limit),
            page: String(page),
          });
          if (typeDept) params.set("type_department", typeDept);
          const apiRes = await breezewayFetch(
            `/public/inventory/v1/task/?${params.toString()}`,
            { method: "GET" }
          );
          if (!apiRes.ok) {
            const txt = await apiRes.text().catch(() => "");
            console.warn(`[BZW] home ${homeId} page ${page} q=${params.toString()}: ${apiRes.status}`, txt.slice(0, 300));
            return out;
          }
          const data = await apiRes.json().catch(() => ({}));
          const items = Array.isArray(data.results)
            ? data.results
            : Array.isArray(data) ? data : [];
          out.push(...items);
          const totalPages = Number(data.total_pages || 0);
          if (totalPages && page >= totalPages) break;
          if (items.length < limit) break;
        }
        return out;
      }

      // 3) Pool de concurrencia.
      const queue = homes.slice();
      const allTasks = [];
      const workers = Array.from({ length: concurrency }, async () => {
        while (queue.length) {
          const h = queue.shift();
          if (!h) break;
          try {
            const ts = await fetchHomeTasks(h.id);
            allTasks.push(...ts);
          } catch (e) {
            console.warn(`[BZW] err home ${h.id}:`, e?.message || e);
          }
        }
      });
      await Promise.all(workers);

      // 4) Convertir a la forma "alert".
      // Schema real Breezeway: home_id (number), name, type_department,
      // finished_at, finished_by:{id,name}|null, scheduled_date, created_at, status.
      const alerts = allTasks.map((t) => {
        const homeId = t.home_id;
        const homeName = propIdx.get(homeId)?.name || `Alojamiento ${homeId || "?"}`;
        const finished = !!t.finished_at;
        return {
          kind: "task-historical",
          event_type: finished ? "task-completed" : "task",
          title: finished
            ? `✅ Aseo terminado: ${homeName}`
            : `📝 ${t.name || t.type_department || "Tarea"}: ${homeName}`,
          last_updated: t.finished_at || t.scheduled_date || t.updated_at || t.created_at,
          property: { id: homeId, name: homeName },
          task: {
            id: t.id,
            name: t.name,
            type: t.type_department,
            finished_at: t.finished_at,
            finished_by: t.finished_by?.name || null,
            status: t.status,
            scheduled_date: t.scheduled_date,
            report_url: t.report_url,
          },
          raw: t,
        };
      });
      alerts.sort((a, b) => String(b.last_updated || "").localeCompare(String(a.last_updated || "")));

      res.json({
        ok: true,
        count: alerts.length,
        from: fromIso, to: toIso,
        scanned_homes: homes.length,
        type_department: typeDept || "(any)",
        date_key: key,
        tasks: alerts,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: String(err?.message || err) });
    }
  });

  // ---- IMPORTAR histórico al buffer (para que aparezcan en /alerts) ----
  app.post("/api/breezeway/import-history", async (req, res) => {
    try {
      // Reutiliza la lógica del GET — copia los query a fetch interno.
      const params = new URLSearchParams();
      Object.entries(req.body || {}).forEach(([k, v]) => params.set(k, String(v)));
      const apiRes = await fetch(`http://127.0.0.1:${process.env.PORT || 8080}/api/breezeway/tasks?${params.toString()}`);
      const data = await apiRes.json().catch(() => ({}));
      if (!apiRes.ok || !data.ok) {
        return res.status(apiRes.status || 500).json(data);
      }
      // Empuja cada task al buffer de recentAlerts (deduplicando por task.id).
      const seenTaskIds = new Set(
        recentAlerts.map((a) => a.task?.id).filter(Boolean)
      );
      let inserted = 0;
      for (const alert of data.tasks) {
        if (seenTaskIds.has(alert.task?.id)) continue;
        recentAlerts.unshift({ ...alert, received_at: new Date().toISOString() });
        inserted++;
      }
      // Respeta el límite del buffer.
      if (recentAlerts.length > MAX_ALERTS) recentAlerts.length = MAX_ALERTS;
      res.json({
        ok: true,
        inserted,
        scanned: data.count,
        total_in_buffer: recentAlerts.length,
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: String(err?.message || err) });
    }
  });

  // ---- BOOTSTRAP HISTÓRICO: trae N tareas y las persiste al sheet ----
  // Reutiliza la lógica del histórico (api/breezeway/tasks) y empuja al
  // sheet en bloques de 500 vía action=breezeway_alerts_bulk (1 escritura
  // por bloque). Mucho más rápido que persistir una por una.
  app.post("/api/breezeway/bootstrap-history", async (req, res) => {
    try {
      const fromIso = String(req.query?.from || "2026-01-01");
      const toIso   = String(req.query?.to   || new Date().toISOString().slice(0,10));
      // Por default filtramos por scheduled_date (Fecha límite) — así las
      // tasks PENDIENTES de hoy también se incluyen aunque aún no tengan
      // finished_at. Antes el default era finished_at, lo que excluía
      // completamente todo lo que estuviera en curso o por hacerse.
      const key = String(req.query?.key || "scheduled_date");
      const url = process.env.CHECKIN_WEB_APP_URL;
      if (!url) return res.status(500).json({ ok:false, error:"CHECKIN_WEB_APP_URL no configurado." });

      // Reutilizamos /tasks armando la query interna.
      const port = process.env.PORT || 8080;
      const tasksRes = await fetch(`http://127.0.0.1:${port}/api/breezeway/tasks?from=${fromIso}&to=${toIso}&key=${key}`);
      const tasksData = await tasksRes.json();
      if (!tasksData.ok) return res.status(500).json({ ok:false, error:"fetch tasks falló", inner: tasksData });
      const tasks = tasksData.tasks || [];
      if (!tasks.length) return res.json({ ok:true, inserted:0, scanned:0, message:"Sin tasks en el rango." });

      // Lookup de bookings Lodgify para enriquecer con Fecha entrada/salida.
      // 1 sola llamada al Apps Script para todos los bookings; se construye
      // un mapa lodgifyId → {arrival, departure}.
      let bookingsMap = new Map();
      try {
        const r = await fetch(url, {
          method: "POST",
          headers: { "Content-Type": "text/plain;charset=utf-8" },
          body: JSON.stringify({ action: "lodgify_list" }),
          redirect: "follow",
        });
        const j = await r.json().catch(() => ({}));
        // El Apps Script devuelve {ok, bookings:[...]} (el frontend lo usa así).
        const rows = Array.isArray(j.bookings) ? j.bookings
                   : Array.isArray(j.rows) ? j.rows
                   : Array.isArray(j.reservations) ? j.reservations : [];
        for (const b of rows) {
          const id = String(b.Id || b.id || b["Lodgify Id"] || "").trim();
          if (!id) continue;
          bookingsMap.set(id, {
            arrival:   b.DateArrival   || b["Fecha de ingreso"] || "",
            departure: b.DateDeparture || b["Fecha de salida"]  || "",
          });
        }
        console.log(`📚 Lodgify lookup: ${bookingsMap.size} bookings indexados (${rows.length} rows raw)`);
      } catch (e) {
        console.warn("⚠️ No se pudo cargar Lodgify para enriquecer fechas:", e?.message || e);
      }

      // Aplanar al schema del sheet — USA flatAlertForSheet para consistencia
      // con el path de webhook (un solo source-of-truth para los nombres de
      // campos). Solo agregamos arrival_date/departure_date enriquecidos.
      const flat = tasks.map(t => {
        const lodId = t.raw?.linked_reservation?.external_reservation_id
                   || t.raw?.linked_reservation?.external_id
                   || "";
        const booking = lodId ? bookingsMap.get(String(lodId)) : null;
        const alert = {
          ...t,
          _arrival_date: booking?.arrival || "",
          _departure_date: booking?.departure || "",
        };
        const row = flatAlertForSheet(alert);
        row.received_at = t.task?.finished_at || t.task?.scheduled_date || new Date().toISOString();
        return row;
      });
      // DEDUPE defensivo dentro del batch: si BZW API devuelve el mismo task
      // dos veces, o si dos workers procesaron homes superpuestos, agrupamos
      // por (task_id, event_type, finished_at) y conservamos el último.
      const seenKeys = new Map();
      for (const r of flat) {
        const k = String(r.task_id || "") + "|" +
                  String(r.event_type || "") + "|" +
                  String(r.finished_at || "");
        if (!r.task_id) continue;
        seenKeys.set(k, r); // last-write-wins
      }
      const dedupedFlat = Array.from(seenKeys.values());
      const internalDupes = flat.length - dedupedFlat.length;
      if (internalDupes > 0) {
        console.log(`[BZW] dedupe interno: ${internalDupes} duplicados eliminados del batch (${flat.length} → ${dedupedFlat.length})`);
      }
      // Ordena ASCENDENTE por scheduled_date (primero las más viejas) ANTES
      // del bulk insert. Apps Script appendea al final del sheet, y
      // listBreezewayAlerts_ devuelve las ÚLTIMAS N filas — así la cola del
      // sheet (lo que el frontend ve) contiene las tasks más recientes,
      // incluidas las pendientes de hoy.
      dedupedFlat.sort((a, b) => String(a.scheduled_date || a.received_at || "")
                                 .localeCompare(String(b.scheduled_date || b.received_at || "")));
      // Reasigna flat para que el resto del código funcione igual
      flat.length = 0;
      Array.prototype.push.apply(flat, dedupedFlat);

      // Bloques de 500 para no pasar el límite de payload + ejecución de
      // Apps Script (6 min). 500 × 4 = 2000, dentro del budget.
      const CHUNK = 500;
      let inserted = 0, skipped = 0;
      for (let i = 0; i < flat.length; i += CHUNK) {
        const slice = flat.slice(i, i + CHUNK);
        const r = await fetch(url, {
          method: "POST",
          headers: { "Content-Type": "text/plain;charset=utf-8" },
          body: JSON.stringify({ action: "breezeway_alerts_bulk", alerts: slice }),
          redirect: "follow",
        });
        const txt = await r.text();
        let j = {};
        try { j = JSON.parse(txt); } catch (_) { j = { ok:false, raw: txt.slice(0,200) }; }
        if (!j.ok) {
          return res.status(500).json({ ok:false, error:"bulk insert falló", chunk_index: i, breezeway_response: j });
        }
        inserted += j.inserted || 0;
        skipped  += j.skipped  || 0;
      }
      res.json({ ok:true, inserted, skipped, scanned: flat.length, from: fromIso, to: toIso });
    } catch (err) {
      res.status(500).json({ ok:false, error: String(err?.message || err) });
    }
  });

  // ---- CLEANUP: borra filas de prueba del sheet ----
  app.post("/api/breezeway/cleanup-test-rows", async (_req, res) => {
    try {
      const url = process.env.CHECKIN_WEB_APP_URL;
      if (!url) return res.status(500).json({ ok:false, error:"CHECKIN_WEB_APP_URL no configurado." });
      const r = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "text/plain;charset=utf-8" },
        body: JSON.stringify({ action: "breezeway_alerts_cleanup" }),
        redirect: "follow",
      });
      const txt = await r.text();
      let j = {};
      try { j = JSON.parse(txt); } catch (_) { return res.status(500).json({ ok:false, error:"non-JSON", raw: txt.slice(0,200) }); }
      res.json(j);
    } catch (err) {
      res.status(500).json({ ok:false, error: String(err?.message || err) });
    }
  });

  // ---- DEBUG: respuesta cruda de Breezeway para una sola propiedad ----
  // Para diagnosticar el formato real de la API. Quitar después.
  app.get("/api/breezeway/_debug-tasks", async (req, res) => {
    try {
      const q = req.query || {};
      const home_id = q.home_id || "1254057";
      const params = new URLSearchParams({ home_id: String(home_id) });
      ["finished_at","scheduled_date","created_at","updated_at","type_department","limit","status","page"].forEach(k => {
        if (q[k] != null && String(q[k]) !== "") params.set(k, String(q[k]));
      });
      if (!params.has("limit")) params.set("limit", "5");
      const url = `/public/inventory/v1/task/?${params.toString()}`;
      const apiRes = await breezewayFetch(url, { method: "GET" });
      const text = await apiRes.text();
      let json = null;
      try { json = JSON.parse(text); } catch (_) {}
      res.json({
        ok: apiRes.ok,
        status: apiRes.status,
        url_called: `https://api.breezeway.io${url}`,
        response: json || text.slice(0, 2000),
      });
    } catch (err) {
      res.status(500).json({ ok: false, error: String(err?.message || err) });
    }
  });

  console.log("🌬️  Rutas Breezeway montadas en /api/breezeway/*");
}

export { getAccessToken, breezewayFetch, recentAlerts };
