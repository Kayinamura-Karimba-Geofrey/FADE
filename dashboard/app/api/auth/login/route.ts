import { API_BASE, problem, writeSession, type SessionPayload } from "@/lib/session";

type LoginBody = { username?: string; password?: string; code?: string };

export async function POST(request: Request) {
  let body: LoginBody;
  try {
    body = (await request.json()) as LoginBody;
  } catch {
    return problem(400, "INVALID_JSON", "request body must be JSON");
  }
  if (!body.username || !body.password) {
    return problem(400, "INVALID_CREDENTIALS", "username and password are required");
  }

  const res = await fetch(`${API_BASE}/v1/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username: body.username, password: body.password, code: body.code ?? "" }),
    cache: "no-store",
  });

  if (!res.ok) {
    // Pass the problem through, but never echo the submitted password.
    const text = await res.text();
    return new Response(text, {
      status: res.status,
      headers: { "Content-Type": res.headers.get("Content-Type") ?? "application/problem+json" },
    });
  }

  const session = (await res.json()) as SessionPayload;
  await writeSession(session);
  // Tokens stay server-side; the browser only learns who it signed in as.
  return Response.json({ user: session.user, scopes: session.scopes ?? [] });
}
