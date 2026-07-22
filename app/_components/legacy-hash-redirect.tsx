"use client";

import { useEffect } from "react";

export function LegacyHashRedirect() {
  useEffect(() => {
    if (window.location.hash.toLowerCase() !== "#search") return;
    window.location.replace(`/search/${window.location.search}`);
  }, []);

  return null;
}
