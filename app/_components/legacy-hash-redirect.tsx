"use client";

import { useEffect } from "react";

export function LegacyHashRedirect() {
  useEffect(() => {
    if (window.location.hash.toLowerCase() !== "#search") return;
    window.location.replace(`/search/events/${window.location.search}`);
  }, []);

  return null;
}
