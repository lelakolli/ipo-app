const CACHE = 'ipo-center-v20';
const SHELL = ['/', '/manifest.json', '/static/icons/icon-512.png'];

self.addEventListener('install', (e) => {
  self.skipWaiting();
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)).catch(() => {}));
});

self.addEventListener('activate', (e) => {
  // wipe old caches (v1's cache-first copy of '/' hides app updates)
  e.waitUntil(
    caches.keys()
      .then((names) => Promise.all(names.filter((n) => n !== CACHE).map((n) => caches.delete(n))))
      .then(() => clients.claim())
  );
});

self.addEventListener('fetch', (e) => {
  const url = new URL(e.request.url);
  if (e.request.method !== 'GET' || url.pathname.startsWith('/api/')) return; // always network

  const isPage = e.request.mode === 'navigate' || url.pathname === '/' || url.pathname.endsWith('.html');
  if (isPage) {
    // NETWORK-FIRST for pages: always show the latest app when online,
    // fall back to cache only when offline
    e.respondWith(
      fetch(e.request).then((resp) => {
        if (resp.ok) {
          const copy = resp.clone();
          caches.open(CACHE).then((c) => c.put(e.request, copy)).catch(() => {});
        }
        return resp;
      }).catch(() => caches.match(e.request).then((hit) => hit || caches.match('/')))
    );
    return;
  }

  // static assets (icon, manifest): cache-first is fine
  e.respondWith(
    caches.match(e.request).then((hit) =>
      hit ||
      fetch(e.request).then((resp) => {
        if (resp.ok) {
          const copy = resp.clone();
          caches.open(CACHE).then((c) => c.put(e.request, copy)).catch(() => {});
        }
        return resp;
      })
    )
  );
});

// ---- background push notifications (work even when the app is closed) ----
self.addEventListener('push', (e) => {
  let d = {};
  try { d = e.data.json(); } catch (_) { d = { title: 'IPO Center', body: e.data ? e.data.text() : '' }; }
  e.waitUntil(self.registration.showNotification(d.title || 'IPO Center', {
    body: d.body || '',
    icon: '/static/icons/icon-512.png',
    badge: '/static/icons/icon-512.png',
    data: { url: d.url || '/' },
    tag: 'ipo-' + Date.now(),
  }));
});

self.addEventListener('notificationclick', (e) => {
  e.notification.close();
  const url = (e.notification.data && e.notification.data.url) || '/';
  e.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then((list) => {
      for (const c of list) {
        if (c.url.startsWith(self.location.origin)) return c.focus();
      }
      return clients.openWindow(url);
    })
  );
});
