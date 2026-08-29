# Theming

Every colour the interface renders comes from one file:
[`app/styles/tokens-color.css`](../app/styles/tokens-color.css). Changing the
product's look is an edit to that file and nothing else.

## The two tiers

The file declares two kinds of custom property, and the difference between them
is the whole design.

### Tier 1 — the primitive palette

Names such as `--gray-300`, `--green-700`, `--blue-600`. Each is a raw hex
literal: a hue family plus a lightness step, where a lower number is lighter.
Primitives say what a colour *is* and nothing about where it belongs.

**Nothing outside `app/styles/tokens-color.css` may reference a primitive.** A
rule that reads `var(--green-700)` has hard-coded a hue into a component, which
is exactly the coupling the token layer exists to remove; it also means the
dark theme cannot move it. Primitives exist only so that a tier 2 token has
something to point at.

The steps are not invented. They were derived from the literal inventory the
stylesheet layer started with — some 1,500 hex occurrences across
`app/globals.css` and the CSS modules — and chosen so that every literal the
product used more than twice lands within about 24 RGB units of a step. Nine
hue families survived that exercise: gray, slate, green, blue, amber, orange,
red, purple, and a small set of extended categorical hues (teal, pink, indigo,
brown, olive) that only the chart ramp reaches.

### Tier 2 — the semantic tokens

Names such as `--bg-surface`, `--fg-muted`, `--status-error`, `--chrome-bar`.
Each names the **role** a colour plays, never the colour itself. These are the
only names `app/globals.css`, the CSS modules, and component code may use.

The roles are grouped as:

| Group | Tokens |
| --- | --- |
| Backgrounds | `--bg-canvas`, `--bg-surface`, `--bg-subtle`, `--bg-raised`, `--bg-inverse` |
| Foregrounds | `--fg-text`, `--fg-strong`, `--fg-muted`, `--fg-faint`, `--fg-inverse`, `--fg-link` |
| Borders | `--border`, `--border-strong`, `--border-focus` |
| Accent | `--accent`, `--accent-hover`, `--accent-soft` |
| Status | `--status-success`, `--status-info`, `--status-warning`, `--status-error`, `--status-neutral`, each with a `-soft` background wash |
| Event severity | `--level-info`, `--level-warn`, `--level-error`, `--level-debug` |
| Charts | `--chart-series-1` through `--chart-series-12` |
| Chrome | `--chrome-bar`, `--chrome-appbar`, `--chrome-hover` |
| Interaction | `--highlight`, `--selection`, `--focus-ring` |

Status tokens describe the outcome of an operation the product performed — a
job that succeeded, a token that is throttled. Event severity tokens describe
the `level` field carried by the log data itself, and are deliberately separate:
an error-level event in a healthy search is not a failed search.

## The rule

> No literal colour may appear outside `app/styles/tokens-*.css`.

That means no `#rrggbb`, no `rgb()`, `rgba()`, `hsl()`, or `color()`, and no
named CSS colour, anywhere in `app/globals.css`, a `*.module.css`, an inline
`style` attribute, or a TypeScript constant that feeds one. `npm run lint:css`
reports the remaining violations.

Two smaller rules follow from it:

- A component reads tier 2 only. If no semantic token fits, the right move is
  to add one — with a role name and a one-line comment — not to reach past it
  into the palette.
- Every token carries a one-line comment naming its role. A token whose role
  cannot be stated in one line is usually two tokens.

## Naming

- Primitives: `--<hue>-<step>`, step ascending from light to dark, `50`
  through `900`, with `0` reserved for white and `950` for the deepest
  neutral. Intermediate steps (`150`, `450`, `550`) exist where the inventory
  demanded them.
- Semantic tokens: `--<group>-<role>`, with the group prefix (`bg`, `fg`,
  `status`, `level`, `chart`, `chrome`) shared by every token in it. Modifiers
  come last: `-hover`, `-strong`, `-soft`.
- No token name mentions a hue. `--status-error`, never `--status-red`.

## Adding a theme

A theme restates tier 2 and leaves tier 1 alone.

1. Add a block keyed on the root attribute, next to the dark block already in
   `app/styles/tokens-color.css`:

   ```css
   :root[data-theme="high-contrast"] {
     color-scheme: light;
     --bg-canvas: var(--gray-0);
     /* … every semantic token whose value differs from the light default … */
   }
   ```

2. Set `color-scheme` so form controls, scrollbars, and the browser's own
   surfaces follow the theme.
3. Point each token at a **primitive**, not at a literal. If the theme needs a
   shade the palette does not have, add the primitive first.
4. Restate only what changes. A token the theme omits inherits the light
   default, which is usually right for the categorical chart ramp: those twelve
   hues are chosen to separate from each other, not from the background.
5. Nothing in the product sets `data-theme` yet. The dark block ships as a
   proof that the split holds and as the target for a toggle when one lands;
   until then it changes no pixel.

## Legacy aliases

The pre-refactor names — `--muted`, `--surface`, `--green-strong`, and their
peers — are still declared at the bottom of the light block, each pointing at
its semantic replacement so no call site broke while the layer was introduced.
They are frozen: nothing may be added to that block, and each one disappears as
its call sites are rewritten to the semantic token.
