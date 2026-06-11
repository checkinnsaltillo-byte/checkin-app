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
async function authenticate() {
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

function handleTaskEvent(payload) {
  const { event_type, task = {}, last_updated } = payload;
  const home = task.home || {};
  const key = `task:${task.id}`;
  if (isDuplicate(key, last_updated)) return null;

  // Nos interesa principalmente cuando el aseo queda terminado.
  const isCompleted = event_type === "task-completed" || Boolean(task.finished_at);
  if (!isCompleted) {
    // Otros eventos de tarea: los registramos pero sin disparar alerta "lista".
    return pushAlert({
      kind: "task",
      event_type,
      title: `Tarea: ${task.name || task.id}`,
      detail: `${home.name || "—"} · estado: ${task.status?.name || task.status?.code || "?"}`,
      property: { id: home.id, name: home.name },
      task: { id: task.id, name: task.name, status: task.status?.name },
      raw: payload,
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
    title: `✅ Aseo terminado: ${home.name || "Alojamiento " + home.id}`,
    detail: `Tarea "${task.name || ""}" finalizada${task.finished_at ? " a las " + task.finished_at : ""}`,
    property: { id: home.id, name: home.name },
    task: {
      id: task.id,
      name: task.name,
      type: task.type?.name,
      finished_at: task.finished_at,
      finished_by: task.finished_by?.name || task.finished_by,
    },
    raw: payload,
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
        handleTaskEvent(body);
      } else {
        handlePropertyStatusEvent(body);
      }
    } catch (err) {
      console.error("❌ Error procesando webhook Breezeway:", err?.message || err);
    }
  });

  // ---- Consultar alertas recientes (para la prueba/UI) ----
  app.get("/api/breezeway/alerts", (req, res) => {
    const limit = Math.min(Number(req.query?.limit) || 25, MAX_ALERTS);
    res.json({ ok: true, count: recentAlerts.length, alerts: recentAlerts.slice(0, limit) });
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
