# Theming

How the styling layer is put together, and where to edit it when the product
should look different.

`app/layout.tsx` loads exactly two stylesheets, in this order:

1. `app/styles/index.css`, which `@import`s the token layer:
   `tokens-color.css` (the colour tiers) then `tokens-scale.css` (everything
   that does not vary by theme).
2. `app/globals.css`, the application stylesheet, followed by the six CSS
   modules the components import for themselves.

Only the first of those is meant to carry a literal. A retheme should be an
edit to `app/styles`, not a search across 7,600 lines of rules.

Every colour the interface renders comes from one file:
[`app/styles/tokens-color.css`](../app/styles/tokens-color.css). Recolouring
the product is an edit to that file alone; the non-colour scales live beside it
in `tokens-scale.css`, described under [Scales](#scales) below.

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

## Scales

`app/styles/tokens-scale.css` holds the non-colour primitives. Each step was
picked from the literals the stylesheets ship today, so migrating a rule onto a
token is a substitution rather than a redesign. The tables below record which
literal maps to which token, and every mapping that will deliberately move a
pixel is called out.

### Spacing

Eight steps on a 4px base. Padding, gap and margin literals currently run
unbroken from 1px to 94px across 191 distinct padding values and 29 distinct
gap values, so a step cannot cover them exactly: each literal rounds to its
nearest step, and a tie rounds **down** so dense surfaces stay dense.

| Token | Value | Replaces |
| --- | --- | --- |
| `--space-1` | 4px | 1px, 2px, 3px, 4px, 5px |
| `--space-2` | 8px | 6px, 7px, 8px, 9px |
| `--space-3` | 12px | 10px, 11px, 12px, 13px |
| `--space-4` | 16px | 14px, 15px, 16px, 17px |
| `--space-5` | 20px | 18px, 19px, 20px, 21px |
| `--space-6` | 24px | 22px through 27px |
| `--space-7` | 32px | 28px through 35px |
| `--space-8` | 40px | 36px and up |

This is the mapping with the widest blast radius in the whole cleanup: 10px
alone appears 170 times and moves to 12px. Migrate it surface by surface behind
`npm run test:visual`, never in one pass.

### Radius

| Token | Value | Replaces |
| --- | --- | --- |
| `--radius-sm` | 2px | 1px, 2px — Splunk's near-square corner, and already the most common of the thirteen distinct radii in use, at 24 declarations |
| `--radius-md` | 8px | 7px, 8px, 9px |
| `--radius-lg` | 12px | 10px, 12px |
| `--radius-pill` | 999px | capsules and fully rounded ends |

`border-radius: 50%` stays a literal. It means "circle", and its rendered
radius depends on the box rather than on this scale, so it is not a step.

### Type

The product's body size is 10px, so the ramp is deliberately bottom-heavy and
`--text-xs`, not `--text-md`, is the default UI size. Seven steps cover the
twenty distinct `font-size` literals in use.

| Token | Value | Replaces | Declarations covered |
| --- | --- | --- | --- |
| `--text-xxs` | 9px | 7px, 8px, 9px | 130 |
| `--text-xs` | 10px | 10px | 344 |
| `--text-sm` | 11px | 11px | 60 |
| `--text-md` | 12px | 12px, 13px | 38 |
| `--text-lg` | 14px | 14px, 15px | 25 |
| `--text-xl` | 16px | 16px, 17px | 34 |
| `--text-xxl` | 20px | 18px through 25px, 28px | 38 |

The one `clamp(32px, 4vw, 55px)` on the sign-in headline stays literal: it is a
fluid display size, not a step on a UI ramp.

### Stacking

Six layers replace thirty distinct `z-index` values. The ladder is ordered
`base < sticky < dropdown < modal < drawer < toast`, which is the order
`app/globals.css` ships today — a mobile drawer really does sit above the modal
layer, because the two never open together and the drawer owns the screen when
it is open. The gaps of 100 leave room for a component to stack its own parts
without borrowing the next layer's number.

| Token | Value | Replaces | Examples |
| --- | --- | --- | --- |
| `--z-base` | 1 | 1–8 | `.modal-card`, `.editor-gutter`, `.timeline-selection`, `.verticalDataLabel` |
| `--z-sticky` | 100 | 5, 10, 18, 20, 40, 50, 60, 180, 200 | `.app-bar`, `.product-bar`, `.suite-app-bar`, `.suite-product-bar`, `.fields-rail`, `.search-composer` |
| `--z-dropdown` | 200 | 55, 70, 75, 90, 95, 100, 110, 120, 130, 210, 260 | `.floating-menu`, `.completion-menu`, `.time-popover`, `.field-inspector`, `.suite-popover`, and the dismiss scrims that back them |
| `--z-modal` | 300 | 300 (`.modal-layer`) | `.modal-layer` |
| `--z-drawer` | 400 | 300, 310, 320 | `.suite-mobile-drawer`, `.search-mobile-drawer`, `.time-picker-mobile-backdrop` and their backdrops |
| `--z-toast` | 500 | 500 | `.toast` |

Two pairs change relative order when the ladder is applied, and both need a
visual check during the migration rather than a blind substitution:

- `.menu-dismiss` (55) currently sits **below** `.product-bar` (60); on the
  ladder the scrim moves above the product bar.
- `.fields-rail` and `.field-inspector` each declare two values (10/100 and
  40/110) for their desktop and mobile forms. Both forms land on the same pair
  of steps, so the mobile override becomes redundant and should be deleted, not
  retokenised.

`.skip-link` (1000) is the one documented exception. It has to be reachable
above every layer including a toast, so it keeps a literal above the ladder.

### Elevation

| Token | Value | Replaces |
| --- | --- | --- |
| `--shadow-sm` | `0 1px 4px rgb(21 36 45 / 9%)` | the 1–2px ambient shadows on cards and rows |
| `--shadow-md` | `0 3px 9px rgb(21 35 43 / 24%)` | the 3–7px lift on menus and popovers |
| `--shadow-lg` | `0 10px 30px rgb(18 29 36 / 18%), 0 2px 7px rgb(18 29 36 / 12%)` | the outgoing `--shadow`, plus the 8–18px drops on drawers and modals |

`--shadow-lg` is byte-identical to the `--shadow` it replaces, so the six rules
that already read a token do not move when `--shadow` is retired.

### Focus

`--focus-ring` is a colour, so it is declared in `tokens-color.css` with the
rest of tier 2 and not here; a theme has to be able to move it. It currently
resolves to `--blue-550` (`#2f8ac1`), the literal the primary focus outline
already uses, so nothing shifts today.

The focus blues are not yet down to one. Beyond that outline, the stylesheets
also carry `#317fa6`, `#3b88b5`, `#3b83a6` and `rgb(47 120 158)` on focus
states, and the alpha outline `rgb(42 120 158 / 28%)` has no primitive of its
own. Collapsing them onto `--focus-ring` — their midpoint `#3b83a6` moves no
single state by more than 12 units on a channel — is migration work, listed
under [Known debt](#known-debt-in-the-token-layer).

### Motion

| Token | Value | Replaces |
| --- | --- | --- |
| `--dur-fast` | 100ms | 90ms, 100ms, 120ms — interaction feedback |
| `--dur-base` | 180ms | 140ms, 180ms — surface transitions |
| `--ease` | `ease-out` | the dominant easing, 10 of 16 declarations |

Loader and spinner cadences (0.7s–1.8s) are animation timing rather than
interaction feedback and stay literal, as does the `0.01ms` that
`prefers-reduced-motion` uses to collapse an animation.

### Fonts

`--font-sans` and `--font-mono` moved out of the `:root` block in
`app/globals.css` with their values unchanged. They live with the scales
because neither varies by theme.

## Breakpoints

A breakpoint cannot be a custom property: `@media` is evaluated before the
cascade, so `@media (max-width: var(--bp-md))` never matches. The canon is
therefore enforced by lint instead, in `.stylelintrc.json`, and written down
here.

| Width | Meaning |
| --- | --- |
| `1240px` | wide desktop → desktop: the outermost content column stops growing |
| `980px` | desktop → compact: side rails collapse and toolbars wrap |
| `760px` | compact → mobile: the layout goes single-column and drawers replace rails |
| `480px` | mobile → narrow: dense tables and button rows stack |

Every media query is written `max-width`, so the canon reads largest-first and
each rule overrides the one above it. The single exception is the
`(pointer: coarse) and (min-width: 761px)` guard for touch tablets, which
cannot be expressed with `max-width`; `761px` is the exclusive complement of
`760px` and is the only `min-width` the lint allows.

`.stylelintrc.json` pins `max-width` and `min-width` to those values, so an
off-canon breakpoint is reported by `npm run lint:css`. The stylesheets still
carry seven of them — 1120px, 800px, 650px, 520px, 430px, 420px and one
`max-height: 650px` — and folding each onto the nearest step is part of the
migration, not of this phase.

## Known debt in the token layer

`npm run lint:css` reports the colour literals inside `app/styles` itself:
`color-no-hex` and the `rgb(` disallow-list apply to every file, and a
primitive tier has nowhere else to put a colour. Phase 5 cannot flip those
rules to errors until `.stylelintrc.json` gains an `overrides` entry exempting
`app/styles/tokens-*.css`; that exemption is what makes the rule above
machine-checkable rather than a convention.

The shadow tokens in `tokens-scale.css` are the same tension seen from the
scale side: their geometry is a scale but their ink is a colour, and they will
move onto a tier-1 colour primitive once the palette names one. Until then they
hold the literals the stylesheets ship today, because a shadow that changes
colour changes every elevated surface. `app/globals.css` still declares the
outgoing `--shadow` beside them, identical in value to `--shadow-lg`, until the
rules that read it are migrated.

Four focus blues remain uncollapsed. `--focus-ring` resolves to the primary
outline's own literal today; `#317fa6`, `#3b88b5`, `#3b83a6` and
`rgb(47 120 158)` still appear on other focus states, and the alpha outline
`rgb(42 120 158 / 28%)` has no primitive at all — it will need `color-mix()` or
an alpha token. Folding them onto one ring is migration work.

Three legacy colour aliases — `--orange`, `--yellow` and `--black` — have no
call sites left anywhere in `app/`, `lib/` or `integration/`. They point at
primitives rather than at semantic tokens because no semantic role fits, and
the migration phase should simply delete them.
