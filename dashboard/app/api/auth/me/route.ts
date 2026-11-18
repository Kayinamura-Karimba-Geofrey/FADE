import { API_BASE, refreshSession, readSession } from "@/lib/session";

async function fetchProfile(token: string) {
  const res = await fetch(`${API_BASE}/v1/auth/me`, {
    headers: { Authorization: `Bearer ${token}` },
    cache: "no-store",
  });
  if (!res.ok) return null;
  return (await res.json()) as Record<string, unknown>;
}

export async function GET() {
  const { access } = await readSession();
  if (access) {
    const profile = await fetchProfile(access);
    if (profile) return Response.json(profile);
  }
  const refreshed = await refreshSession();
  if (!refreshed) {
    return new Response(null, { status: 401 });
  }
  const profile = await fetchProfile(refreshed);
  if (!profile) return new Response(null, { status: 401 });
  return Response.json(profile);
}
