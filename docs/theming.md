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

`--focus-ring` (`#3b83a6`) replaces four near-identical focus blues — `#317fa6`,
`#3b88b5`, `#3b83a6` and `rgb(47 120 158)`. It is their midpoint, so no focus
state shifts by more than 12 units on any channel.

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
primitive tier has nowhere else to put a colour. The shadow and focus tokens in
`tokens-scale.css` are the scale side of that: their geometry is a scale but
their ink is a colour, and they will move onto a tier-1 colour primitive once
the palette names one. Until then they hold the literals the stylesheets ship
today, because a shadow that changes colour changes every elevated surface.
