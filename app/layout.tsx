import type { Metadata, Viewport } from "next";
import type { ReactNode } from "react";

import { OPEN_SPLUNK_SOURCE_REVISION } from "@/lib/build-identity";

import "./styles/index.css";
import "./globals.css";

export const metadata: Metadata = {
  title: {
    default: "Open Splunk",
    template: "%s | Open Splunk",
  },
  description: "SPL-compatible log search powered by ClickHouse",
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  viewportFit: "cover",
  // The browser paints its own chrome from this before any stylesheet loads, so
  // it cannot read a custom property: this literal is the one place `--chrome-bar`
  // (app/styles/tokens-color.css) has to be restated. Change both together.
  themeColor: "#1e252b",
};

export default function RootLayout({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html
      lang="en"
      data-open-splunk-revision={OPEN_SPLUNK_SOURCE_REVISION}
    >
      <body>{children}</body>
    </html>
  );
}
