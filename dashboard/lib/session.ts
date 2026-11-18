import { cookies } from "next/headers";

export const ACCESS_COOKIE = "fade_access";
export const REFRESH_COOKIE = "fade_refresh";

export const API_BASE = process.env.API_BASE_URL ?? "http://localhost:8080";

const ACCESS_MAX_AGE = 60 * 15;

export type Profile = {
  id: string;
  username: string;
  display_name: string;
  role: string;
  active: boolean;
  mfa_enabled?: boolean;
  scopes?: string[];
};

export type SessionPayload = {
  access_token: string;
  refresh_token: string;
  expires_in: number;
  user: Profile;
  scopes?: string[];
};

function cookieOptions(maxAge: number) {
  return {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "strict" as const,
    path: "/",
    maxAge,
  };
}

/**
 * Persists the token pair in httpOnly cookies. The browser never sees a token:
 * only the BFF route handlers and the proxy can read these cookies.
 */
export async function writeSession(session: SessionPayload): Promise<void> {
  const jar = await cookies();
  jar.set(ACCESS_COOKIE, session.access_token, cookieOptions(ACCESS_MAX_AGE));
  jar.set(REFRESH_COOKIE, session.refresh_token, cookieOptions(60 * 60 * 12));
}

export async function clearSession(): Promise<void> {
  const jar = await cookies();
  jar.set(ACCESS_COOKIE, "", cookieOptions(0));
  jar.set(REFRESH_COOKIE, "", cookieOptions(0));
}

export async function readSession(): Promise<{ access?: string; refresh?: string }> {
  const jar = await cookies();
  return { access: jar.get(ACCESS_COOKIE)?.value, refresh: jar.get(REFRESH_COOKIE)?.value };
}

/** Rotates the refresh token pair. Returns the new access token, or null. */
export async function refreshSession(): Promise<string | null> {
  const { refresh } = await readSession();
  if (!refresh) return null;
  const res = await fetch(`${API_BASE}/v1/auth/refresh`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ refresh_token: refresh }),
    cache: "no-store",
  });
  if (!res.ok) {
    await clearSession();
    return null;
  }
  const session = (await res.json()) as SessionPayload;
  await writeSession(session);
  return session.access_token;
}

export function problem(status: number, code: string, detail: string): Response {
  return new Response(JSON.stringify({ status, code, detail }), {
    status,
    headers: { "Content-Type": "application/problem+json" },
  });
}
