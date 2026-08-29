import type { ComponentPropsWithoutRef, ReactNode } from "react";

/**
 * The tones `.button--*` in `app/globals.css` paints.
 *
 * `secondary` is the quiet filled control, `ghost` the borderless one that only
 * shows a ground on hover. There is no `link` tone: text that navigates is an
 * anchor, and giving it a button tone made three call sites render a link that
 * looked pressable and was not.
 */
export type ButtonVariant = "default" | "primary" | "secondary" | "danger" | "ghost";

/** Row heights, off the same `--space` scale the CSS measures with. */
export type ButtonSize = "default" | "compact";

export interface ButtonClassNameOptions {
  /** Extra classes for a feature's own layout, never for tone or size. */
  className?: string;
  /** Stretches the control to its container, for a stacked mobile action. */
  block?: boolean;
  /** Square, padding-free box for a control whose whole label is one glyph. */
  icon?: boolean;
  size?: ButtonSize;
  variant?: ButtonVariant;
}

/**
 * Builds the class list for a button-shaped element.
 *
 * Exported separately from the component because a `<Link>` and an `<a>` are
 * buttons too as far as the stylesheet is concerned, and wrapping Next's router
 * link in another component to get its classes would cost a client boundary for
 * a string.
 */
export function buttonClassName({
  block = false,
  className,
  icon = false,
  size = "default",
  variant = "default",
}: ButtonClassNameOptions = {}): string {
  return [
    "button",
    variant === "default" ? "" : `button--${variant}`,
    size === "default" ? "" : `button--${size}`,
    icon ? "button--icon" : "",
    block ? "button--block" : "",
    className ?? "",
  ].filter(Boolean).join(" ");
}

export interface ButtonProps extends Omit<ComponentPropsWithoutRef<"button">, "className">, ButtonClassNameOptions {
  children?: ReactNode;
}

/**
 * A `<button>` that composes the primitive's classes from props.
 *
 * Worth a component only where the variant is *computed* -- a control that
 * turns destructive, or a toolbar that decides its own emphasis. Where the
 * variant is a constant, `className="button button--primary"` says the same
 * thing with one fewer indirection, and the codebase deliberately keeps both.
 */
export function Button({ block, children, className, icon, size, type = "button", variant, ...props }: ButtonProps) {
  return (
    <button {...props} className={buttonClassName({ block, className, icon, size, variant })} type={type}>
      {children}
    </button>
  );
}
