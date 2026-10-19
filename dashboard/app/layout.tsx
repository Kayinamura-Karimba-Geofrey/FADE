import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "FADE Analyst Dashboard",
  description: "Fraud and Anomaly Detection Engine - analyst workbench",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className="min-h-screen bg-gray-950 text-gray-100 antialiased">{children}</body>
    </html>
  );
}
