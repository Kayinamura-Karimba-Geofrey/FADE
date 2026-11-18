import type { Metadata } from "next";
import Link from "next/link";
import "./globals.css";
import Providers from "./providers";

export const metadata: Metadata = {
  title: "FADE Dashboard",
  description: "Fraud and Anomaly Detection Engine analyst console",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-gray-50 text-gray-900 antialiased">
        <Providers>
          <a
            href="#main"
            className="sr-only focus:not-sr-only focus:absolute focus:left-4 focus:top-4 focus:rounded focus:bg-white focus:px-3 focus:py-2 focus:shadow"
          >
            Skip to content
          </a>
          <header className="border-b bg-white">
            <nav aria-label="Primary" className="mx-auto flex max-w-6xl items-center gap-6 px-4 py-3">
              <span className="font-semibold">FADE</span>
              <Link className="text-sm hover:underline" href="/queue">
                Review queue
              </Link>
              <Link className="text-sm hover:underline" href="/metrics">
                Metrics
              </Link>
              <form action="/api/auth/logout" method="post" className="ml-auto">
                <button
                  type="submit"
                  className="rounded border px-3 py-1 text-sm hover:bg-gray-100"
                >
                  Sign out
                </button>
              </form>
            </nav>
          </header>
          <main id="main" className="mx-auto max-w-6xl px-4 py-6">
            {children}
          </main>
        </Providers>
      </body>
    </html>
  );
}
