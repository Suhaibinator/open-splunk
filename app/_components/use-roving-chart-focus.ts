"use client";

import { type KeyboardEvent, useRef, useState } from "react";

/** Roving tabindex + active-point state shared by the keyboard-navigable charts. */
export function useRovingChartFocus(count: number) {
  const [activeIndex, setActiveIndex] = useState<number | null>(null);
  const [focusIndex, setFocusIndex] = useState(0);
  const itemRefs = useRef<Array<HTMLButtonElement | null>>([]);

  function moveFocus(index: number) {
    const nextIndex = Math.max(0, Math.min(count - 1, index));
    setFocusIndex(nextIndex);
    setActiveIndex(nextIndex);
    itemRefs.current[nextIndex]?.focus({ preventScroll: true });
  }

  function handleKeyDown(event: KeyboardEvent<HTMLButtonElement>, index: number) {
    if (event.key === "ArrowRight" || event.key === "ArrowDown") moveFocus(index + 1);
    else if (event.key === "ArrowLeft" || event.key === "ArrowUp") moveFocus(index - 1);
    else if (event.key === "Home") moveFocus(0);
    else if (event.key === "End") moveFocus(count - 1);
    else if (event.key === "Escape") setActiveIndex(null);
    else return;
    event.preventDefault();
  }

  function handleFocus(index: number) {
    setFocusIndex(index);
    setActiveIndex(index);
  }

  return { activeIndex, setActiveIndex, focusIndex, itemRefs, handleKeyDown, handleFocus };
}
