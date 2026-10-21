function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function apiTarget(env) {
  const raw = env.CUSTOMER_HTTP_DASHBOARD_API ?? env.DASHBOARD_API_URL;
  if (!raw) return null;
  return raw.endsWith("/") ? raw.slice(0, -1) : raw;
}

const v3Routes = new Set([
  "/api/v3/overview", "/api/v3/market", "/api/v3/cycles",
  "/api/v3/execution", "/api/v3/economics", "/api/v3/system",
  "/api/v3/research", "/api/v3/diagnostic-export", "/api/v3/health",
]);

function allowedRoute(pathname) {
  return v3Routes.has(pathname) || /^\/api\/v3\/cycles\/[^/]+$/.test(pathname);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname.startsWith("/api/")) {
      if (!["GET", "HEAD", "OPTIONS"].includes(request.method)) {
        return json({ detail: "V3 看板只读。" }, 405);
      }
      if (!allowedRoute(url.pathname)) {
        return json({ detail: "不允许访问此 API。" }, 404);
      }
      if (request.method === "OPTIONS") {
        return new Response(null, { status: 204, headers: { Allow: "GET, HEAD, OPTIONS" } });
      }
      // A service token authenticates the worker to upstream, not the visitor.
      // Fail closed until deployment-level visitor access protection is verified.
      if (env.DASHBOARD_PRIVATE_ACCESS_CONFIRMED !== "true") {
        return json({ detail: "未确认访客访问保护，V3 API 代理已禁用。" }, 503);
      }
      const base = apiTarget(env);
      if (!base) {
        return json({
          detail: "Sites 页面已发布，但实时看板 API 尚未绑定。",
        }, 503);
      }

      const upstream = new URL(`${base}${url.pathname}${url.search}`);
      const headers = new Headers(request.headers);
      headers.set("accept", "application/json");
      const token = env.DASHBOARD_API_TOKEN;
      if (token) headers.set("x-entropy-dashboard-token", token);

      try {
        const response = await fetch(new Request(upstream, {
          method: request.method,
          headers,
        }));
        const responseHeaders = new Headers(response.headers);
        responseHeaders.set("cache-control", "no-store");
        return new Response(response.body, {
          status: response.status,
          headers: responseHeaders,
        });
      } catch {
        return json({ detail: "实时看板 API 当前不可用。" }, 502);
      }
    }

    const asset = await env.ASSETS.fetch(request);
    if (asset.status !== 404 || request.method !== "GET") return asset;

    const accept = request.headers.get("accept") ?? "";
    if (accept.includes("text/html")) {
      return env.ASSETS.fetch(new Request(new URL("/index.html", request.url), request));
    }
    return asset;
  },
};
