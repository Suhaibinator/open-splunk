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

That is the target, not yet the state. The layer exists —
[`app/styles/tokens-color.css`](../app/styles/tokens-color.css) declares every
colour role the product has, and `tokens-scale.css` the non-colour scales
described under [Scales](#scales) below — but no consumer reads it yet. The
pre-refactor names still resolve through it, so the shipped render is
unchanged, while `npm run lint:css` still reports 1,496 hex literals outside
`app/styles`. Recolouring the product becomes a one-file edit when those call
sites are rewritten, which is the phase after this one.

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
| Chrome | `--chrome-bar`, `--chrome-appbar`, `--chrome-hover`, `--chrome-fg` |
| Interaction | `--highlight`, `--selection`, `--focus-ring` |

Status tokens describe the outcome of an operation the product performed — a
job that succeeded, a token that is throttled. Event severity tokens describe
the `level` field carried by the log data itself, and are deliberately separate:
an error-level event in a healthy search is not a failed search.

`--fg-inverse` and `--chrome-fg` are likewise two roles rather than one, and
the dark theme is why. `--fg-inverse` is the ink on `--bg-inverse`, a surface
that flips ends of the gray ramp with the theme; `--chrome-fg` is the ink on
the two application bars, which are dark in both themes. A single token would
have to be light and dark at once, and the first draft of the dark block —
where one token carried both jobs — painted every bar label in the bar's own
colour at a contrast ratio of 1.00:1. `scripts/token-grammar.test.mjs` now
checks each foreground against every ground its role comment names, in both
themes.

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
  demanded them. The step is a claim about lightness, so it is measured: a
  family whose CIE L\* stops falling as its step number rises is a ladder that
  lies about which of two colours is darker, and the grammar test rejects it.
- Semantic tokens: `--<group>-<role>`, with the group prefix (`bg`, `fg`,
  `status`, `level`, `chart`, `chrome`) shared by every token in it. Modifiers
  come last: `-hover`, `-strong`, `-soft`.
- No token name mentions a hue. `--status-error`, never `--status-red`.
- One family, one kind of value. `var(--type-md)` is a length and
  `var(--fg-muted)` is a colour, and the prefix says which before the value is
  looked up. This is why the type scale is `--type-*` and not `--text-*`: the
  legacy aliases `--text` and `--text-strong` are inks, and a family cannot
  mean both.

Declarations inside the token files are **not** alphabetised, and that is
deliberate. `tokens-color.css` runs hue family by hue family and, within a
family, light step to dark, because that order is the palette: a ramp read out
of alphabetical order (`--gray-0`, `--gray-100`, `--gray-150`, `--gray-50`) is
unreadable as a ramp. `tokens-scale.css` alphabetises inside each banner group,
where the members have no natural order. Everywhere else in the styling layer
the repository's rule stands — declarations inside a rule block are
alphabetised — so this exemption is scoped to the two token files and should
not be "fixed" into line with it.

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
4. Restate only what changes, and change everything the theme owes. Both halves
   are checked by `scripts/token-grammar.test.mjs`: a restatement that resolves
   to the light value is dead weight every future theme has to keep in step, and
   a themeable token the block forgets keeps its light primitive on a dark
   ground. A token the theme omits inherits the light default, which is right
   for the categorical chart ramp and only for it: those twelve hues are chosen
   to separate from each other, not from the background, and the test exempts
   them by name. The four `--level-*` swatches are the near miss — they encode
   data like the ramp does, but they are read as text and as small fills, so the
   dark block lifts all four together to the `-300` tier rather than leaving
   half of them behind.
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
`--type-xs`, not `--type-md`, is the default UI size. Seven steps cover the
twenty distinct `font-size` literals in use.

The family is `--type-*` rather than the `--text-*` these steps were first
written as, because `--text` and `--text-strong` are colours: with both
families in the layer, `var(--text-…)` told a reader nothing about whether they
were writing an ink or a length. The scale has no call sites yet, so renaming
it costs nothing; the two aliases still have hundreds, so they keep their
names until they are deleted.

| Token | Value | Replaces | Declarations covered |
| --- | --- | --- | --- |
| `--type-xxs` | 9px | 7px, 8px, 9px | 130 |
| `--type-xs` | 10px | 10px | 344 |
| `--type-sm` | 11px | 11px | 60 |
| `--type-md` | 12px | 12px, 13px | 38 |
| `--type-lg` | 14px | 14px, 15px | 25 |
| `--type-xl` | 16px | 16px, 17px | 34 |
| `--type-xxl` | 20px | 18px through 25px, 28px | 38 |

The one `clamp(32px, 4vw, 55px)` on the sign-in headline stays literal: it is a
fluid display size, not a step on a UI ramp.

### Stacking

Nine steps replace the twenty-nine distinct `z-index` values `app/globals.css`
ships across fifty-five declarations, ordered

`base < sticky < bar < dropdown-scrim < dropdown < modal < drawer-scrim < drawer < toast`

which is the order the stylesheet ships today everywhere but one pair, called
out under [What the ladder changes](#what-the-ladder-changes). A drawer sits
above the modal layer because the two never open together and a drawer owns the
screen while it is open. The gaps of 100 leave room for a component to stack its
own parts without borrowing the next layer's number.

Two of the nine steps exist because an overlay and the transparent scrim that
dismisses it are **not** one layer. Where they share a step, which one wins is
decided by DOM order, and in four of this product's six pairs the scrim is the
later sibling: it would paint over the surface it backs and swallow every click
meant for the menu. A scrim therefore always sits one step below the thing it
dismisses. The mapping below is derived from each declaration's role, not from
its number — two rules with the same `z-index` today can land on different
steps, and two with different numbers can land on the same one.

| Token | Value | Role | Replaces |
| --- | --- | --- | --- |
| `--z-base` | 1 | one lift above an element's in-flow siblings | `.modal-card` (1), `.statistics-table th` (1), `.donut-chart > span` (1) |
| `--z-sticky` | 100 | sticky page furniture, still inside the scroll flow | `.resource-toolbar label` (5), desktop `.fields-rail` (10), `.search-composer` (18), `.search-title-row` (20) |
| `--z-bar` | 200 | the persistent application bars | `.app-bar` (50), `.product-bar` (60), `.suite-app-bar` (180), `.suite-product-bar` (200) |
| `--z-dropdown-scrim` | 300 | the transparent buttons that close an open menu | `.menu-dismiss` (55), `.fields-mobile-dismiss` (95), `.suite-dismiss` (210) |
| `--z-dropdown` | 400 | menus and popovers, and the containers raised to carry one | the `:has(.floating-menu)` raises on `.search-title-row`, `.job-strip`, `.result-view-header`, `.timeline-toolbar` and `.event-toolbar` (70), `.search-composer:has(.time-popover)` (75), `.floating-menu` (90), mobile `.fields-rail` (100), `.completion-menu` (120), `.time-popover` (130), `.resource-action-menu` (20), `.suite-popover` (260) |
| `--z-modal` | 500 | the modal layer | `.modal-layer` (300) |
| `--z-drawer-scrim` | 600 | the backdrop behind a mobile drawer | `.suite-mobile-backdrop` (300), `.search-mobile-backdrop` (300), `.time-picker-mobile-backdrop` (310) |
| `--z-drawer` | 700 | mobile drawers | `.suite-mobile-drawer` (320), `.search-mobile-drawer` (320), mobile `.time-popover` (320) |
| `--z-toast` | 800 | transient notifications | `.toast` (500) |

`.skip-link` (1000) is the one documented exception. It has to be reachable
above every layer including a toast, so it keeps a literal above the ladder.

#### Values that stay literal

A `z-index` is only a page layer when it competes with the rest of the page.
Twenty-three of the fifty-five do not: they order an element against its
siblings inside a stacking context its parent already opened, so a ladder step
would either flatten a local order or promise an escape the element cannot
make. These keep their literals:

- the chart's own parts, which are a four-step ladder inside one chart —
  `.time-series-chart__inspect` (1), `__crosshair` (2), `__marker` (3),
  `__tooltip` (4);
- the SPL editor's — `.spl-editor.focused` (2), `.editor-gutter` (2),
  `.spl-editor textarea` (3), `.editor-meta` (4);
- the single-step lifts inside a component: `.timeline-keyboard-control` (2),
  `.timeline-bars > span` (2), `.timeline-selection` (3),
  `.timechart-column-bars > button > b` (2),
  `.top-value > button:first-child` (2), `.top-value > div` (3),
  `.signin-story-copy` (2), `.signin-story > footer` (2),
  `.statistics-scroll-hint` (3), `.fields-topbar` (3), the sticky footer inside
  `.time-popover` (2), and the two mobile drawer headers (2);
- `.field-inspector` (40 desktop, 110 mobile). It is rendered inside
  `.fields-rail`, which opens a stacking context in both forms, so neither
  number reaches the page: both mean "above the rail's own contents". This is
  the one place the two forms really are redundant, and the mobile `z-index`
  can be deleted rather than retokenised — the rest of that mobile rule, which
  repositions the panel, stays.

The six CSS modules declare ten more, all between 1 and 8 and all inside
their own component's stacking context. They stay literal for the same reason.

`.fields-rail` itself is the opposite case and its mobile override must be
kept: on the desktop it is in-flow furniture (10, `--z-sticky`), and on mobile
it is a fixed overlay that has to sit above `.fields-mobile-dismiss`
(100 over 95, so `--z-dropdown` over `--z-dropdown-scrim`). Collapsing the two
forms onto one step would drop the mobile rail behind its own scrim.

#### What the ladder changes

**One pair changes relative order.** `.menu-dismiss` (55) currently sits below
`.product-bar` (60); on the ladder every dismiss scrim is above every bar, so
the scrim moves over the product bar. The suite shell already behaves that way
(`.suite-dismiss` 210 over `.suite-product-bar` 200), so the substitution makes
the two shells agree rather than inventing a third behaviour.

**Five groups collapse values that have an order today.** Inside a step, DOM
order decides, so each of these wants a look at the rendered page rather than a
blind substitution:

| Step | Tied by the ladder | Today | After, by DOM order |
| --- | --- | --- | --- |
| `--z-sticky` | `.search-title-row` (20), `.search-composer` (18), desktop `.fields-rail` (10) | title row, then composer, then rail | rail over composer over title row |
| `--z-bar` | `.product-bar` (60), `.app-bar` (50) | product bar over app bar | app bar over product bar |
| `--z-bar` | `.suite-product-bar` (200), `.suite-app-bar` (180) | product bar over app bar | app bar over product bar |
| `--z-dropdown` | `.floating-menu` (90), mobile `.fields-rail` (100), `.completion-menu` (120), `.time-popover` (130), `.suite-popover` (260) | in that order | whichever is later in the DOM |
| `--z-drawer-scrim` | `.time-picker-mobile-backdrop` (310) over the two mobile backdrops (300) | time-picker backdrop on top | whichever is later in the DOM |

The two bar rows are the ones to watch: in both shells the product bar is
rendered before the app bar, so a tie inverts them. They are adjacent strips
that only overlap where a bar's own popover is open, and every such popover is
on `--z-dropdown`, above both — but that is an argument for checking the page,
not for skipping it. The `--z-dropdown` row ties five surfaces that are rarely
open at once; the `--z-drawer-scrim` row ties backdrops that belong to
different shells.

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
resolves to `--blue-450` (`#2f8ac1`), the literal the primary focus outline
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

`.stylelintrc.json` carries an `overrides` entry exempting
`app/styles/tokens-*.css` from `color-no-hex` and the `rgb()` disallow-list:
those rules exist to push every literal into the primitive tier, so firing them
there would ask the palette to point at itself, and Phase 5 could never flip
them to errors. The exemption is scoped to the two token files and to those two
rules — a `font-family` literal in a token file is still reported — which is
what makes "no literal outside `app/styles/tokens-*.css`" a machine-checkable
rule rather than a convention.

The shadow tokens in `tokens-scale.css` are the same tension seen from the
scale side: their geometry is a scale but their ink is a colour, and they will
move onto a tier-1 colour primitive once the palette names one. Until then they
hold the literals the stylesheets ship today, because a shadow that changes
colour changes every elevated surface. `app/globals.css` still declares the
outgoing `--shadow` beside them, identical in value to `--shadow-lg`, until the
rules that read it are migrated.

Three text pairings are short of WCAG AA (4.5:1) and are inherited rather than
introduced: `--fg-faint` on `--bg-surface` is 3.10:1 in the light theme and
3.90:1 in the dark, and `--fg-link` on `--bg-canvas` is 4.47:1 — three
hundredths short. `scripts/token-grammar.test.mjs` checks only the pairings a
token's own role comment promises, so these are not test failures; moving them
is a palette change with call sites behind it, which belongs to a migration
phase rather than to this one.

Four focus blues remain uncollapsed. `--focus-ring` resolves to the primary
outline's own literal today; `#317fa6`, `#3b88b5`, `#3b83a6` and
`rgb(47 120 158)` still appear on other focus states, and the alpha outline
`rgb(42 120 158 / 28%)` has no primitive at all — it will need `color-mix()` or
an alpha token. Folding them onto one ring is migration work.

Eight legacy colour aliases have no call sites left anywhere in `app/`, `lib/`
or `integration/` outside `app/styles` itself: `--orange`, `--yellow`,
`--black`, `--blue-soft`, `--border-dark`, `--green-soft`, `--surface-raised`
and `--surface-subtle`. The first three point at primitives rather than at
semantic tokens because no semantic role fits them; the other five already
point at their replacement. All eight can be deleted outright — the migration
phase does not have to rewrite a single rule for them. The count is worth
re-running rather than trusting: `grep -rn 'var(--NAME)' app lib integration |
grep -v '^app/styles/'`.

The fourteen that are still read are `--app-bar`, `--app-bar-hover`, `--blue`,
`--canvas`, `--faint`, `--green`, `--green-strong`, `--muted`,
`--product-bar`, `--red`, `--red-soft`, `--surface`, `--text` and
`--text-strong`; `--muted` alone has thirty-three call sites.

### Colour mappings that move a pixel

The `Replaces` columns under [Scales](#scales) call out every substitution that
will deliberately change a rendered value. The colour half needs the same list,
because six semantic tokens are pointed at the nearest palette step rather than
at the exact literal they will replace. Each is a decision, not an accident —
the palette is what makes a theme possible, and a step per one-off literal
would defeat it — but each will move a baseline when Phase 2 substitutes it,
and that churn should be read as expected rather than as a regression.

| Token | Resolves to | Replaces | Distance |
| --- | --- | --- | --- |
| `--chart-series-1` | `--green-600` `#5f9c3a` | `TIME_SERIES_COLORS[0]` `#5f9f3a` | 3 |
| `--chart-series-2` | `--blue-600` `#2878a8` | `TIME_SERIES_COLORS[1]` `#2f7fa6` | 7 |
| `--chart-series-3` | `--amber-400` `#dda229` | `TIME_SERIES_COLORS[2]` `#e49a2c` | 8 |
| `--chart-series-5` | `--red-400` `#c84f48` | `TIME_SERIES_COLORS[4]` `#c6534c` | 4 |
| `--chrome-bar` | `--slate-900` `#1e252b` | `.suite-product-bar` `#1d252b` | 1 |
| `--chrome-appbar` | `--slate-700` `#3f464c` | `.suite-app-bar` `#424a50` | 4 |

Distance is the largest single-channel difference. The baselines to expect
churn in are the search workspace's visualization chart and the product-shell
bars on every exported page. `TIME_SERIES_COLORS` lives in
`app/search-workspace/charts/time-series-line-chart.tsx`; series 4 and 6
through 12 already match it exactly, as do the twenty-three legacy aliases,
which are pinned channel-by-channel in
`integration/visual/css-contracts.spec.ts` along with the semantic tokens that
stand in for a literal.

The categorical ramp has a second, older problem it inherits from
`TIME_SERIES_COLORS` and does not fix: its bottom end is thin. Pairwise CIE76
ΔE puts `--chart-series-6` and `--chart-series-10` 10.4 apart,
`--chart-series-1` and `--chart-series-12` 12.8, and `--chart-series-2` and
`--chart-series-8` 16.4, where a categorical palette usually wants 20 or more.
A twelve-series legend therefore has two pairs that read as one colour.
Widening the ramp is a design change rather than a refactor, so it is recorded
here rather than made quietly.
