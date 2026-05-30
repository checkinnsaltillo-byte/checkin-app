// Service worker — Check-inn PWA
// Maneja:
//  · install/activate (sin cache)
//  · push (mostrar notificación + badge rojo en el ícono)
//  · notificationclick (abre o foca la app)

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener('fetch', (event) => {
  // No interceptamos peticiones — comportamiento online idéntico al actual.
  return;
});

self.addEventListener('push', (event) => {
  let payload = {};
  if (event.data) {
    try { payload = event.data.json(); } catch (_e) {
      try { payload = { title: 'Check-inn', body: event.data.text() }; } catch(__){}
    }
  }
  const title = payload.title || 'Check-inn Saltillo';
  const options = {
    body: payload.body || 'Tienes un nuevo aviso.',
    icon: payload.icon || 'icon-192.png',
    badge: payload.badge || 'icon-192.png',
    tag: payload.tag || 'checkinn-default',
    renotify: !!payload.renotify,
    data: { url: payload.url || './', ...(payload.data || {}) }
  };
  const tasks = [self.registration.showNotification(title, options)];
  // Badge en el ícono (Android instalado, iOS 16.4+ PWA, desktop)
  const badgeCount = Number(payload.badgeCount);
  if (!isNaN(badgeCount) && badgeCount > 0 && self.navigator && typeof self.navigator.setAppBadge === 'function') {
    tasks.push(self.navigator.setAppBadge(badgeCount).catch(()=>{}));
  } else if (badgeCount === 0 && self.navigator && typeof self.navigator.clearAppBadge === 'function') {
    tasks.push(self.navigator.clearAppBadge().catch(()=>{}));
  }
  event.waitUntil(Promise.all(tasks));
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const targetUrl = (event.notification.data && event.notification.data.url) || './';
  event.waitUntil((async () => {
    const all = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    for (const c of all) {
      try {
        const u = new URL(c.url);
        if (u.pathname.includes('/registro')) {
          await c.focus();
          if (typeof c.navigate === 'function' && targetUrl !== './') {
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
