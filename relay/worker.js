/**
 * OneCinema: видео-релей на Cloudflare Workers.
 *
 * Зачем: тяжёлый видеотрафик должен идти мимо Railway (egress $0.05/GB), но
 * браузер не может тянуть поток с CDN источника напрямую (CORS + нужны
 * Referer/Origin плеера). Воркер принимает запрос вида
 *     https://<worker>/?url=<поток>&ref=<origin плеера>
 * тянет upstream с нужными заголовками и отдаёт клиенту с CORS. Плейлисты
 * HLS/DASH переписываются так, что сегменты тоже идут через воркер.
 *
 * Деплой (бесплатный тариф, карта не нужна):
 *   1. https://dash.cloudflare.com -> Workers & Pages -> Create -> Worker.
 *   2. Вставить этот код, Deploy.
 *   3. Скопировать адрес воркера (https://<имя>.<аккаунт>.workers.dev)
 *      и задать его на Railway: переменная окружения RELAY_ORIGIN.
 *
 * Если релей недоступен или CDN его режет — фронтенд сам падает обратно на
 * серверный /api/proxy, так что сайт не ломается.
 */

// CDN семейства stravers (плеер hdrezka) отдаёт мастер-плейлист 403, пока не
// получит эти заголовки. Значения те же, что в server.py (STRAVERS_BEARER /
// STRAVERS_FP). Фронтенд помечает такие потоки параметром sv=1.
const STRAVERS_FP = "3d3f8df92bfcf386fa0e4d0274f8284467749bade62f2f71a23c200c41df7844";
const STRAVERS_BEARER = (
  "pXzvbyDGLYyB6VkwsWZDv3iMKZtsXNzpzRyxZUcsKHXxsSeaYakbo3hw9mBFRc5VQTpqAX6BW8aDEqyLaHYcXSQi" +
  "V6KHYTVTK6MYRphNAy5sBjtrevqkDzKmLqNdfMZGEU9NELjmtKfZy3RNGzCd767sNh1mXEj4tCcvqndHtzmwAbZNkhm4gh" +
  "DEasodotMBewypNQ56uotJAQGX11csfeRfBAPk8DcUWWkkqzxca8vbnEw12vUFbBzT6hz8ZB3F3dzUhUXoL2cr1WM1bXQAr" +
  "RCS1MUNMz3X5WDMMQoZKxj2AMTRqp7QQX4dDB9B7VzEZTmyFULhm1AcHHMkoMvSVvKYoBoAKLycYAgMHeD4ECJcGEAGpnkJ" +
  "hrV57zQ7"
);

export default {
  async fetch(request) {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders() });
    }
    if (request.method !== "GET" && request.method !== "HEAD") {
      return new Response("method not allowed", { status: 405, headers: corsHeaders() });
    }

    const self = new URL(request.url);
    const target = self.searchParams.get("url");
    const ref = self.searchParams.get("ref");
    const sv = self.searchParams.get("sv") === "1";
    if (!target || !/^https?:\/\//i.test(target)) {
      return new Response("no url", { status: 400, headers: corsHeaders() });
    }

    const upstreamHeaders = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
      "Accept": "*/*",
    };
    if (ref) {
      const origin = ref.replace(/\/+$/, "");
      upstreamHeaders["Referer"] = origin + "/";
      upstreamHeaders["Origin"] = origin;
    }
    if (sv) {
      upstreamHeaders["Authorizations"] = "Bearer " + STRAVERS_BEARER;
      upstreamHeaders["Accepts-Controls"] = STRAVERS_FP;
    }
    const range = request.headers.get("Range");
    if (range) upstreamHeaders["Range"] = range;

    let upstream;
    try {
      upstream = await fetch(target, {
        method: "GET",
        headers: upstreamHeaders,
        redirect: "follow",
      });
    } catch (e) {
      return new Response("upstream error: " + e.message, { status: 502, headers: corsHeaders() });
    }

    // Плейлист/манифест: переписываем ссылки, чтобы сегменты шли через воркер.
    const ctype = (upstream.headers.get("Content-Type") || "").toLowerCase();
    const base = upstream.url || target;
    if (ctype.includes("mpegurl") || /\.m3u8(\?|$)/i.test(base)) {
      const text = await upstream.text();
      const body = rewriteM3u8(text, base, ref, self.origin, sv);
      return new Response(body, {
        status: upstream.status,
        headers: corsHeaders({
          "Content-Type": "application/vnd.apple.mpegurl",
          "Cache-Control": "no-store",
        }),
      });
    }
    if (ctype.includes("dash+xml") || /\.mpd(\?|$)/i.test(base)) {
      const text = await upstream.text();
      const body = rewriteMpd(text, base, ref, self.origin, sv);
      return new Response(body, {
        status: upstream.status,
        headers: corsHeaders({
          "Content-Type": "application/dash+xml",
          "Cache-Control": "no-store",
        }),
      });
    }

    // Сегменты/файлы: потоком, с сохранением Range (перемотка).
    const out = corsHeaders();
    for (const k of ["Content-Type", "Content-Range", "Accept-Ranges", "Last-Modified", "ETag"]) {
      const v = upstream.headers.get(k);
      if (v) out[k] = v;
    }
    // Content-Length не переносим при сжатии: CF раскодирует тело, и размер разойдётся.
    const len = upstream.headers.get("Content-Length");
    if (len && !upstream.headers.get("Content-Encoding")) out["Content-Length"] = len;

    return new Response(request.method === "HEAD" ? null : upstream.body, {
      status: upstream.status,
      headers: out,
    });
  },
};

function corsHeaders(extra) {
  return Object.assign({
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Range, Accept",
    "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
    "Access-Control-Max-Age": "86400",
  }, extra || {});
}

/** Ссылка на этот же воркер для вложенного ресурса. */
function wrap(absUrl, ref, origin, sv) {
  // $Number$/$Time$/$Bandwidth$ в DASH-шаблонах оставляем как есть (dash.js подставит сам)
  const enc = encodeURIComponent(absUrl).replace(/%24/g, "$");
  return origin + "/?url=" + enc + (ref ? "&ref=" + encodeURIComponent(ref) : "")
       + (sv ? "&sv=1" : "");
}

/** Абсолютизирует относительный URI относительно базы плейлиста. */
function abs(base, u) {
  try {
    return new URL(u, base).href;
  } catch (e) {
    return u;
  }
}

function rewriteM3u8(text, base, ref, origin, sv) {
  const lines = text.split(/\r?\n/);
  const out = [];
  for (const line of lines) {
    const s = line.trim();
    if (!s) {
      out.push(line);
      continue;
    }
    if (s.startsWith("#")) {
      // URI="..." внутри тегов (#EXT-X-KEY, #EXT-X-MAP, #EXT-X-MEDIA ...)
      out.push(line.replace(/URI="([^"]+)"/g, (m, p1) => {
        if (p1.startsWith("data:")) return m;
        return 'URI="' + wrap(abs(base, p1), ref, origin, sv) + '"';
      }));
    } else {
      out.push(wrap(abs(base, s), ref, origin, sv));
    }
  }
  return out.join("\n");
}

function rewriteMpd(text, mpdUrl, ref, origin, sv) {
  const bases = [...text.matchAll(/<BaseURL>\s*([^<]+?)\s*<\/BaseURL>/g)].map(m => m[1].trim());
  const base = bases[0] ? abs(mpdUrl, bases[0]) : mpdUrl.replace(/[^/]*$/, "");
  text = text.replace(/<BaseURL>\s*[^<]+?\s*<\/BaseURL>\s*/g, "");

  for (const attr of ["initialization", "media", "sourceURL"]) {
    const re = new RegExp('(\\b' + attr + '=")([^"]+)(")', "g");
    text = text.replace(re, (m, a, u, c) => {
      if (u.startsWith("data:")) return m;
      return a + wrap(abs(base, u), ref, origin, sv) + c;
    });
  }
  // В исходном манифесте $Number$/$Bandwidth$ уже с $; wrap() их сохраняет.
  return text;
}
