import { NextRequest } from "next/server";
import { API_BASE, refreshSession, readSession } from "@/lib/session";

const HOP_BY_HOP = new Set([
  "host",
  "connection",
  "content-length",
  "accept-encoding",
  "cookie",
  "authorization",
]);

async function forward(req: NextRequest, token: string | undefined, body: ArrayBuffer | undefined) {
  const path = req.nextUrl.pathname.replace(/^\/api\/proxy/, "");
  const headers = new Headers();
  req.headers.forEach((value, key) => {
    if (!HOP_BY_HOP.has(key.toLowerCase())) headers.set(key, value);
  });
  if (token) headers.set("Authorization", `Bearer ${token}`);
  return fetch(`${API_BASE}${path}${req.nextUrl.search}`, {
    method: req.method,
    headers,
    body: req.method === "GET" || req.method === "HEAD" ? undefined : body,
    cache: "no-store",
  });
}

/**
 * Authenticated proxy from the browser to the decision API. The access token is
 * read from an httpOnly cookie and retried once after a refresh, so an expired
 * token never surfaces to the analyst as a failed action.
 */
async function handler(req: NextRequest) {
  const body =
    req.method === "GET" || req.method === "HEAD" ? undefined : await req.arrayBuffer();
  const { access } = await readSession();

  let upstream = await forward(req, access, body);
  if (upstream.status === 401) {
    const refreshed = await refreshSession();
    if (refreshed) upstream = await forward(req, refreshed, body);
  }

  const headers = new Headers();
  const contentType = upstream.headers.get("Content-Type");
  if (contentType) headers.set("Content-Type", contentType);
  const idempotentReplay = upstream.headers.get("Idempotent-Replay");
  if (idempotentReplay) headers.set("Idempotent-Replay", idempotentReplay);
  headers.set("Cache-Control", "no-store");

  return new Response(upstream.body, { status: upstream.status, headers });
}

export {
  handler as GET,
  handler as POST,
  handler as PUT,
  handler as PATCH,
  handler as DELETE,
};
