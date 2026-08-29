import Link from "next/link";

/**
 * The product mark.
 *
 * It appeared four times in three shapes -- the product bar, the search bar and
 * the sign-in page's two brands -- with three CSS blocks that had drifted apart
 * on ink and tracking. The markup lives here once; `.wordmark` in
 * `app/styles/primitives/layout.css` carries the bar sizing and `.wordmark--hero`
 * the larger
 * sign-in brand.
 */
interface WordmarkProps {
  /** Extra classes for whatever surface the mark sits on. */
  className?: string;
  href: string;
  /** "hero" is the sign-in brand: larger, unpadded, sized to its text. */
  size?: "bar" | "hero";
}

export function Wordmark({ className, href, size = "bar" }: WordmarkProps) {
  const classes = ["wordmark"];
  if (size === "hero") classes.push("wordmark--hero");
  if (className !== undefined) classes.push(className);
  return (
    <Link className={classes.join(" ")} href={href} aria-label="Open Splunk home">
      <span>open</span><b>&gt;</b><span>splunk</span>
    </Link>
  );
}
