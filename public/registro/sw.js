// Service worker — Check-inn PWA (v2)
// Estrategia:
//  · install: skipWaiting (el SW nuevo toma control inmediato).
//  · activate: clients.claim() + notifica a clientes para que recarguen.
//  · fetch: network-first para navegaciones HTML — esto fuerza que la PWA
//    en iOS NO sirva HTML cacheado por Safari y siempre traiga el último deploy.
//  · push / notificationclick: notificaciones + badge.

const SW_VERSION = "2026-05-30-3";

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
  // Network-first para HTML — evita que Safari/iOS sirva HTML caché viejo.
  event.respondWith((async () => {
    try {
      const fresh = await fetch(req, { cache: "no-store" });
      return fresh;
    } catch (err) {
      // Sin red: intentar lo que haya en caché HTTP del browser
      return fetch(req).catch(() => new Response(
        "<h1>Sin conexión</h1><p>Reintenta cuando tengas red.</p>",
        { headers: { "Content-Type": "text/html; charset=utf-8" }, status: 503 }
      ));
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
  const badgeCount = Number(payload.badgeCount);
  if (!isNaN(badgeCount) && badgeCount > 0 && self.navigator && typeof self.navigator.setAppBadge === "function") {
    tasks.push(self.navigator.setAppBadge(badgeCount).catch(()=>{}));
  } else if (badgeCount === 0 && self.navigator && typeof self.navigator.clearAppBadge === "function") {
    tasks.push(self.navigator.clearAppBadge().catch(()=>{}));
  }
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
