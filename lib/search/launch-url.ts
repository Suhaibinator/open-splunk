interface SearchLaunchOptions {
  earliest?: string;
  latest?: string;
  label?: string;
  run?: boolean;
}

export function searchLaunchHref(query: string, options: SearchLaunchOptions = {}): string {
  const parameters = new URLSearchParams({
    q: query,
    earliest: options.earliest ?? "-24h",
    latest: options.latest ?? "now",
    run: options.run === false ? "0" : "1",
  });
  if (options.label !== undefined) parameters.set("label", options.label);
  return `/search/?${parameters.toString()}`;
}

export function splFromFindInput(value: string): string {
  const trimmed = value.trim();
  if (/\bindex\s*=|\|/i.test(trimmed)) return trimmed;
  if (/^(?:NOT\s+)?[A-Za-z_][A-Za-z0-9_.-]*\s*(?:=|!=|>=|<=|>|<)/i.test(trimmed)) {
    return `index=gradethis ${trimmed}`;
  }
  const escaped = trimmed.replaceAll("\\", "\\\\").replaceAll('"', '\\"');
  return `index=gradethis "${escaped}"`;
}
