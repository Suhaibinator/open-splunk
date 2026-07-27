import type { Metadata, Viewport } from "next";
import type { ReactNode } from "react";

import {
  OPEN_SPLUNK_APPLICATION_VERSION,
  OPEN_SPLUNK_SOURCE_REVISION,
} from "@/lib/build-identity";

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
  themeColor: "#1e252b",
};

export default function RootLayout({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html
      lang="en"
      data-open-splunk-version={OPEN_SPLUNK_APPLICATION_VERSION}
      data-open-splunk-revision={OPEN_SPLUNK_SOURCE_REVISION}
    >
      <body>{children}</body>
    </html>
  );
}
