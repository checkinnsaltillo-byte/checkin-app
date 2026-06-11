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

  console.log("🌬️  Rutas Breezeway montadas en /api/breezeway/*");
}

export { getAccessToken, breezewayFetch, recentAlerts };
