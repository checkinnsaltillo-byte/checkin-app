// Service worker — Check-inn PWA (v3)
// Estrategia:
//  · install: skipWaiting (el SW nuevo toma control inmediato).
//  · activate: clients.claim() + notifica a clientes para que recarguen.
//  · fetch: solo intercepta navegaciones HTML para ofrecer página offline.
//    Usa cache HTTP nativo del browser ({cache:"default"}) — esto evita
//    re-descargar los ~767 KB del index.html en cada visita y deja que
//    el cache-control max-age=600 de GitHub Pages haga su trabajo.
//    Para forzar fresh HTML tras un deploy, el activate notifica a los
//    clientes para que recarguen.
//  · push / notificationclick: notificaciones + badge.

const SW_VERSION = "2026-06-02-1";

self.addEventListener("install", (event) => {
  self.skipWaiting();
});

self.addEventListener("activate", (event) => {
  event.waitUntil((async () => {
    await self.clients.claim();
    // Avisar a clientes activos para que recarguen y obtengan HTML nuevo.
    const all = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
    for (const c of all) {
      try { c.postMessage({ type: "sw-activated", version: SW_VERSION }); } catch(_e){}
    }
  })());
});

self.addEventListener("fetch", (event) => {
  const req = event.request;
  if (req.method !== "GET") return;
  const isNavigation =
    req.mode === "navigate" ||
    (req.headers.get("accept") || "").includes("text/html");
  if (!isNavigation) return;
  // Usar cache HTTP nativo del browser ({cache:"default"}) — respeta el
  // cache-control: max-age=600 que GitHub Pages envía, así una segunda
  // visita dentro de 10 min sirve el HTML cacheado al instante. La
  // protección contra deploy stale viene del clients.claim() + reload
  // en el activate, no de forzar no-store en cada fetch.
  event.respondWith((async () => {
    try {
      return await fetch(req);
    } catch (_err) {
      // Sin red: fallback offline
      return new Response(
        "<h1>Sin conexión</h1><p>Reintenta cuando tengas red.</p>",
        { headers: { "Content-Type": "text/html; charset=utf-8" }, status: 503 }
      );
    }
  })());
});

self.addEventListener("push", (event) => {
  let payload = {};
  if (event.data) {
    try { payload = event.data.json(); } catch (_e) {
      try { payload = { title: "Check-inn", body: event.data.text() }; } catch(__){}
    }
  }
  const title = payload.title || "Check-inn Saltillo";
  const options = {
    body: payload.body || "Tienes un nuevo aviso.",
    icon: payload.icon || "icon-192.png",
    badge: payload.badge || "icon-192.png",
    tag: payload.tag || "checkinn-default",
    renotify: !!payload.renotify,
    data: { url: payload.url || "./", ...(payload.data || {}) }
  };
  const tasks = [self.registration.showNotification(title, options)];
  // Badge en el ícono de la PWA. iOS tiene soporte limitado de setAppBadge
  // desde el SW background, así que también mandamos un postMessage a
  // todos los clientes abiertos para que el frontend lo intente (más
  // confiable cuando la PWA está activa).
  const badgeCount = Number(payload.badgeCount);
  const hasBadgeApi = self.navigator && typeof self.navigator.setAppBadge === "function";
  console.log("[sw] push received. badgeCount=", badgeCount, "setAppBadge available:", hasBadgeApi);
  if (!isNaN(badgeCount) && badgeCount > 0 && hasBadgeApi) {
    tasks.push(
      self.navigator.setAppBadge(badgeCount)
        .then(() => console.log("[sw] setAppBadge OK:", badgeCount))
        .catch((e) => console.warn("[sw] setAppBadge failed:", e))
    );
  } else if (badgeCount === 0 && self.navigator && typeof self.navigator.clearAppBadge === "function") {
    tasks.push(self.navigator.clearAppBadge().catch(()=>{}));
  }
  // Notificar a los clientes activos (si los hay) para que sincronicen
  // el badge desde el cliente — funciona mejor en iOS que el SW solo.
  tasks.push((async () => {
    try {
      const allClients = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
      for (const c of allClients) {
        try { c.postMessage({ type: "push-received", badgeCount: badgeCount }); } catch(_e){}
      }
    } catch(_e){}
  })());
  event.waitUntil(Promise.all(tasks));
});

self.addEventListener("notificationclick", (event) => {
  event.notification.close();
  const targetUrl = (event.notification.data && event.notification.data.url) || "./";
  event.waitUntil((async () => {
    const all = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
    for (const c of all) {
      try {
        const u = new URL(c.url);
        if (u.pathname.includes("/registro")) {
          await c.focus();
          if (typeof c.navigate === "function" && targetUrl !== "./") {
            try { await c.navigate(targetUrl); } catch(_e){}
          }
          return;
        }
      } catch(_e){}
    }
    if (self.clients.openWindow) {
      await self.clients.openWindow(targetUrl);
    }
  })());
});
