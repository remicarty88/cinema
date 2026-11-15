/* OneCinema — service worker.
   Стратегии:
   - страницы: network-first (свежий контент), кэш как офлайн-запас;
   - /api/film: network-first — карточка фильма всегда свежая, кэш только
     как офлайн-запас;
   - каталог/поиск/фильтры/постеры: stale-while-revalidate — открытие берётся
     из кэша мгновенно, обновление прилетает в фоне;
   - картинки (/api/img), иконки, шрифты: cache-first с лимитом размера кэша. */
const CACHE = "k88-v8";
const API_CACHE = "k88-api-v3";
const IMG_CACHE = "k88-img-v1";
const FONT_CACHE = "k88-font-v1";
const KEEP = [CACHE, API_CACHE, IMG_CACHE, FONT_CACHE];
const API_SWR = ["/api/home", "/api/browse", "/api/search", "/api/filters", "/api/posters"];
const API_CACHE_MAX = 300;
const IMG_CACHE_MAX = 400;
const PRECACHE = ["/", "/manifest.json", "/icon-192.png?v=2", "/icon-512.png?v=2", "/apple-touch-icon.png?v=2"];

self.addEventListener("install", e => {
  self.skipWaiting();
  e.waitUntil(
    caches.open(CACHE).then(c => Promise.all(PRECACHE.map(u => c.add(u).catch(() => {}))))
  );
});

self.addEventListener("activate", e => {
  e.waitUntil((async () => {
    const keys = await caches.keys();
    await Promise.all(keys.filter(k => !KEEP.includes(k)).map(k => caches.delete(k)));
    if (self.registration.navigationPreload) {
      try { await self.registration.navigationPreload.enable(); } catch (_) {}
    }
    await self.clients.claim();
  })());
});

self.addEventListener("message", e => {
  if (e.data === "skipWaiting") self.skipWaiting();
});

async function put(cache, req, res, max) {
  try {
    await cache.put(req, res.clone());
    const keys = await cache.keys();
    if (keys.length > max) await cache.delete(keys[0]);
  } catch (e) {}
}

async function networkFirst(req) {
  const cache = await caches.open(API_CACHE);
  try {
    const r = await fetch(req);
    if (r && r.ok) put(cache, req, r, API_CACHE_MAX);
    return r;
  } catch (err) {
    const hit = await cache.match(req);
    if (hit) return hit;
    throw err;
  }
}

async function staleWhileRevalidate(req, cacheName, max) {
  const cache = await caches.open(cacheName);
  const hit = await cache.match(req);
  const net = fetch(req)
    .then(r => { if (r && (r.ok || r.type === "opaque")) put(cache, req, r, max); return r; })
    .catch(() => null);
  if (hit) return hit;
  const fresh = await net;
  if (fresh) return fresh;
  return new Response('{"ok":false,"error":"offline"}',
    { status: 503, headers: { "Content-Type": "application/json" } });
}

async function cacheFirst(req, cacheName, max) {
  const cache = await caches.open(cacheName);
  const hit = await cache.match(req);
  if (hit) return hit;
  const r = await fetch(req);
  if (r && r.ok) put(cache, req, r, max);
  return r;
}

self.addEventListener("fetch", e => {
  const req = e.request;
  if (req.method !== "GET") return;
  const u = new URL(req.url);

  /* шрифты Google: после первого запуска работают и офлайн */
  if (u.hostname === "fonts.googleapis.com" || u.hostname === "fonts.gstatic.com") {
    e.respondWith(staleWhileRevalidate(req, FONT_CACHE, 30));
    return;
  }
  if (u.origin !== location.origin) return;
  if (u.pathname.startsWith("/sw.js")) return;
  if (u.pathname.startsWith("/admin")) return;      // дашборд аналитики — никогда не кэшируем
  if (req.headers.has("range")) return;              // видео и поток — напрямую

  if (u.pathname === "/api/img") {
    e.respondWith(cacheFirst(req, IMG_CACHE, IMG_CACHE_MAX).catch(() => Response.error()));
    return;
  }
  if (/^\/(icon-[\w-]+|apple-touch-icon)\.png$/.test(u.pathname) || u.pathname === "/manifest.json") {
    e.respondWith(cacheFirst(req, CACHE, 20).catch(() => Response.error()));
    return;
  }

  if (u.pathname.startsWith("/api/")) {
    if (u.pathname === "/api/film") e.respondWith(networkFirst(req));
    else if (API_SWR.includes(u.pathname)) e.respondWith(staleWhileRevalidate(req, API_CACHE, API_CACHE_MAX));
    return;                            // остальные API — напрямую
  }

  /* страницы */
  e.respondWith((async () => {
    try {
      const pre = e.preloadResponse ? await e.preloadResponse : null;
      const r = pre || await fetch(req);
      if (r && r.ok && req.mode === "navigate") {
        const copy = r.clone();
        caches.open(CACHE).then(c => c.put("/", copy)).catch(() => {});
      }
      return r;
    } catch (err) {
      const hit = (await caches.match(req)) || (req.mode === "navigate" ? await caches.match("/") : null);
      if (hit) return hit;
      throw err;
    }
  })());
});
