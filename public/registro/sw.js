// Service worker mínimo para que el sitio califique como PWA instalable.
// No cachea recursos — el comportamiento online queda idéntico al actual.
// Más adelante se puede ampliar para push notifications + badging API.

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener('fetch', (event) => {
  // Pasar TODAS las peticiones directamente a la red.
  // (No interceptamos nada para evitar romper Apps Script / GitHub Pages.)
  return;
});
