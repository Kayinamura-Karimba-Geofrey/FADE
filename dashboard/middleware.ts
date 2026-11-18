import { NextRequest, NextResponse } from "next/server";

const PUBLIC = ["/login", "/api/auth/login", "/api/auth/refresh"];

/**
 * Cheap gate: only token presence is checked here (Edge cannot verify the JWT
 * without the signing secret). Expired tokens are refreshed by the BFF routes.
 */
export function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;
  if (PUBLIC.some((p) => pathname === p || pathname.startsWith(`${p}/`))) {
    return NextResponse.next();
  }
  const hasSession =
    request.cookies.has("fade_access") || request.cookies.has("fade_refresh");
  if (hasSession) return NextResponse.next();
  if (pathname.startsWith("/api/")) {
    return new NextResponse(null, { status: 401 });
  }
  const url = request.nextUrl.clone();
  url.pathname = "/login";
  url.searchParams.set("next", pathname);
  return NextResponse.redirect(url);
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
