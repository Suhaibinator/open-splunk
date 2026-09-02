import type { Metadata, Viewport } from "next";
import type { ReactNode } from "react";

import { OPEN_SPLUNK_SOURCE_REVISION } from "@/lib/build-identity";
import { THEME_BOOT_SCRIPT } from "@/lib/theme-preference";

import { ThemeSync } from "./_components/theme-sync";

import "./styles/index.css";

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
    // `data-theme` is written by the boot script below before React hydrates,
    // and the server render cannot know which value that will be, so the
    // attribute is expected to differ on this one element.
    <html
      lang="en"
      data-open-splunk-revision={OPEN_SPLUNK_SOURCE_REVISION}
      suppressHydrationWarning
    >
      <head>
        {/*
          The theme has to be resolved before the first paint, ahead of every
          stylesheet: this is a static export, so nothing server-side can read
          the preference, and a module script would run after the light render
          had already flashed. The script is a fixed string owned by
          lib/theme-preference.ts, held to `resolveTheme` by its unit test.
        */}
        <script dangerouslySetInnerHTML={{ __html: THEME_BOOT_SCRIPT }} />
      </head>
      <body>
        <ThemeSync />
        {children}
      </body>
    </html>
  );
}
