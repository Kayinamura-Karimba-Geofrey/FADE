import { API_BASE, clearSession, readSession } from "@/lib/session";

export async function POST() {
  const { access, refresh } = await readSession();
  if (access && refresh) {
    await fetch(`${API_BASE}/v1/auth/logout`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${access}` },
      body: JSON.stringify({ refresh_token: refresh }),
      cache: "no-store",
    }).catch(() => undefined);
  }
  await clearSession();
  return new Response(null, { status: 204 });
}
