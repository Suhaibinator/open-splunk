# Theming

Open Splunk has one plain-CSS cascade. `app/layout.tsx` imports only
`app/styles/index.css`; that file imports every stylesheet exactly once in a
deliberate order. Do not add CSS imports to components.

Colours, typography, scale radii, page-level stacking, complete shadows,
motion, translucency, and fonts come from the token layer: the base pair
`app/styles/tokens-color.css` and `app/styles/tokens-scale.css`, plus one
`app/styles/tokens-palette-<name>.css` per instance palette. Component styles
still contain ordinary layout geometry and spacing. A checked literal ledger
permits only the intentional scale exceptions described below; it is a ratchet,
not a general exemption.

The theme has two axes. The administrator picks the instance *palette* for
everyone, the sign-in page included; each user keeps their own System / Light /
Dark choice on top of it. Every palette ships a light block and a dark block,
so the boot script, the operating-system switch and the user's Theme menu work
the same under every palette.

## Where a rule lives

Shared elements belong in `app/styles/base.css` or a primitive under
`app/styles/primitives/`. A feature rule belongs in the `*.css` file beside the
feature. Responsive rules stay with their base selectors. The complete cascade
is:

| Order | File | Ownership |
| ---: | --- | --- |
| 1 | `app/styles/tokens-color.css` | primitive palette, semantic colour roles, base dark-theme role overrides |
| 2 | `app/styles/tokens-scale.css` | spacing, radii, type, stacking, elevation, motion, opacity, translucency knobs, fonts |
| 3 | `app/styles/tokens-palette-ember.css` | ember palette: light and dark restatements of colour and scale roles |
| 4 | `app/styles/tokens-palette-glass.css` | glass palette: translucency knobs, borders, purple accent, chrome, radii, shadows |
| 5 | `app/styles/tokens-palette-graphite.css` | graphite palette: 7:1 monochrome inks, gray accent and chrome, ring shadows |
| 6 | `app/styles/tokens-palette-ocean.css` | ocean palette: mist canvas, blue accent, slate-blue chrome, softer scale |
| 7 | `app/styles/tokens-palette-terminal.css` | terminal palette: mono type stack, square corners, ring shadows, phosphor |
| 8 | `app/styles/base.css` | reset, element defaults, focus ring, shared keyframes |
| 9 | `app/styles/primitives/button.css` | `.button` and modifiers |
| 10 | `app/styles/primitives/table.css` | `.table-wrap`, `.table`, modifiers and card mode |
| 11 | `app/styles/primitives/form.css` | fields, labels, form and settings layouts |
| 12 | `app/styles/primitives/modal.css` | dialog shell and modal-only surfaces |
| 13 | `app/styles/primitives/status.css` | `.status` and `.badge` |
| 14 | `app/styles/primitives/layout.css` | product shell, drawer, toasts and suite layouts |
| 15 | `app/styles/primitives/chart.css` | shared chart geometry |
| 16 | `app/styles/primitives/skeleton.css` | shared loading placeholders and reduced-motion behavior |
| 17 | `app/search-workspace/search-editor.css` | search title, editor and time picker |
| 18 | `app/search-workspace/search-job.css` | job strip, tabs and timeline |
| 19 | `app/search-workspace/search-fields.css` | fields rail and events |
| 20 | `app/search-workspace/search-results.css` | patterns, statistics and result grids |
| 21 | `app/admin/admin.css` | administration and knowledge surfaces |
| 22 | `app/activity/activity.css` | activity console |
| 23 | `app/datasets/datasets.css` | dataset cards and field catalog |
| 24 | `app/signin/signin.css` | sign-in page |
| 25 | `app/home.css` | landing page |
| 26 | `app/_components/backend-resource-state.css` | connected-backend empty/error state |
| 27 | `app/dashboards/dashboards.css` | dashboard page rules |
| 28 | `app/analytics/analytics.css` | analytics console; `analytics-` namespace |
| 29 | `app/dashboards/operations-dashboard.css` | operations dashboard; `operations-` namespace |
| 30 | `app/reports/reports.css` | reports and saved searches; `reports-` namespace |
| 31 | `app/reports/alerts.css` | alert management; `alerts-` namespace |
| 32 | `app/search-workspace/components/workspace-dialogs.css` | workspace dialogs; `workspace-dialog-` namespace |
| 33 | `app/search-workspace/panels/visualization-panel.css` | result visualizations; `visualization-` namespace |
| 34 | `app/styles/interaction.css` | coarse-pointer and reduced-motion floors |

The five palette files must follow both base token files: a palette's light
block is written as `:root:where([data-palette="…"])` so it keeps base light's
specificity and beats it by source order alone, which is what lets the base
dark block still outrank it in dark mode. `scripts/style-inventory.mjs`
`tokenCascadeOrder` states that order -- colour, scale, then every palette file
sorted by name -- and the head-order invariant holds `index.css` to it.

Primitives precede features so a feature may supply layout around a primitive.
`interaction.css` is last because its accessibility floors must outrank both.
If a selector crosses feature boundaries, put it with the shared primitive or
at the last relevant point in the cascade and explain the dependency in a
comment.

## One styling lane

Use plain CSS, kebab-case feature prefixes, and BEM modifiers. Do not use CSS
modules, CSS-in-JS, `:global()`, inline colour, or a second implementation of a
shared concept. Express state with `data-*`, `aria-*`, or an `is-*` class.

The prefix names the owning feature, but need not literally equal its directory
name. Examples are `.reports-table`, `.alerts-table`,
`.operations-dashboard-card`, and `.workspace-dialog-history-table`.

## Tokens and themes

### Two tiers

`tokens-color.css` has two tiers:

- Tier 1 is the raw palette: names such as `--gray-300`, `--green-700`,
  `--blue-600`. Each is a raw hex literal:
  a hue family plus a lightness step, where a lower number is
  lighter. Steps sit on the `0`-`950` ladder and the invariants measure each
  family in CIE L\*, so a step number really does say how light a primitive
  is. Primitives say what a colour *is* and nothing about where it belongs,
  and every primitive is a distinct colour.
- Tier 2 names semantic roles such as foreground, surface, border, accent,
  status, syntax, chart-series, skeleton, and chrome colours. All other
  stylesheets read only these roles.
  Point each token at a primitive, not at a literal. A semantic token that
  reads another semantic token inherits a role it was not given, and a theme
  that moves one moves both.

The rule between the tiers:
nothing outside the token layer may reference a primitive. A rule that reads
`var(--green-700)` has hard-coded a hue into a component: no theme block or
palette file can move it, and no screenshot can tell it apart from the
semantic token beside it. Primitives
live only in `tokens-color.css`, and only in its base light block: a palette
that needs a new hue step adds it there, on the ladder, with a comment naming
the palette that uses it, and points its own roles at it. Hue families are
named for the colour and palettes for the look, so no hue family may carry a
palette's name (`--ocean-500` would read as "the ocean palette's step").

To recolour an existing role, change its tier-2 mapping. Add a tier-2 token only
when the role is genuinely new, with a one-line role comment. Never reference a
tier-1 palette token from a component.
Every token carries a one-line comment naming its role. A token whose role
cannot be stated in one line is usually two tokens. (`tokens-scale.css` states
each family's rationale in a banner above it instead; the colour tier and
every palette file comment per token.)

### Naming

- Primitives: `--<hue>-<step>`, step ascending from light to dark, with `0`
  reserved for white and `950` for the deepest neutral. Off-hundred steps
  (`50`, `150`, `250`, `350`, `450`, `550`, `650`, `750`) exist where a role
  needed them; check the ladder before adding one.
- Semantic tokens: a group prefix, then the role, then any modifier. The
  groups, in the order the invariant lists them, are
  accent, bg, border, chart, chrome, fg, level, skeleton, status, syntax;
  `--focus-ring`, `--highlight` and `--selection` are the three interaction
  tokens the grammar leaves ungrouped. Modifiers come last: `-hover`,
  `-bright`, `-strong`, `-soft`, `-subtle`, `-edge`, `-alt`, `-inverse`.
  `scripts/style-invariants.test.mjs` holds that list, so a new group is a
  change to this document and to that test together.
- Scale tokens: the families `alpha`, `backdrop`, `dur`, `ease`, `font`,
  `opacity`, `radius`, `shadow`, `space`, `type` and `z`. One family, one kind
  of value: `var(--type-md)` is a length and `var(--fg-muted)` is a colour, and
  the prefix says which before the value is looked up. The type scale is
  `--type-*` and not `--text-*` because `--text` was once an ink.
- No token name mentions a hue. --status-error, never --status-red. A hue in
  the name freezes the colour into every call site and a theme can no longer
  move it.
- Status tokens describe the outcome of an operation the product performed;
  event severity (`--level-*`) describes the `level` field the log data
  itself carries, and the two stay separate because an error-level event in a
  healthy search is not a failed search. Two roles in one group never resolve
  to the same colour in any scope.

Declarations inside the token files are not alphabetised: they are grouped by
role, in the order a reader looks for them.

### Two axes, four block shapes

`<html>` carries two attributes, `data-theme` (`light` or `dark`) and
`data-palette` (one of the names in `lib/palettes.ts`'s `PALETTES`). Both are
always written explicitly, `classic` included, so a stylesheet never has to
reason about an absent attribute. The token layer contains exactly four block
shapes, and their preludes are exact:

| Scope | Prelude | Specificity |
| --- | --- | --- |
| base light | `:root` | 0,1,0 |
| base dark | `:root[data-theme="dark"]` | 0,2,0 |
| palette light | `:root:where([data-palette="ocean"])` | 0,1,0 |
| palette dark | `:root[data-palette="ocean"][data-theme="dark"]` | 0,3,0 |

The resolution order is `base light < palette light < base dark < palette
dark`. `:where()` on the palette light block is what makes it hold: it keeps
the block at base light's specificity, so source order alone lets it win in
light mode while the base dark block (`0,2,0`) still outranks it in dark mode.
A plain `:root[data-palette="ocean"]` would be `0,2,0`, would tie with base
dark, and -- loaded after it -- would leak the palette's light grounds under
every dark page. The invariants refuse any other prelude, and the Playwright
contract "a palette light restatement never outranks the base dark block"
proves the consequence on the live cascade.

Each of the twelve scopes (six palettes, two modes) therefore resolves a token
through a fixed chain:

- classic light: `[base light]`; classic dark: `[base light, base dark]`;
- `P` light: `[base light, P light]`; `P` dark:
  `[base light, P light, base dark, P dark]`.

`classic` is the base pair itself. `data-palette="classic"` selects nothing,
no `tokens-palette-classic.css` may exist, and no block may select it: it is
the promise every other chain resolves through. Every other palette owns
exactly one file holding exactly one light block and one dark block, colour and
scale restatements together, and the base files hold only base blocks.

Three more rules keep the layer honest:

- A block other than base light restates only names base light declares.
  A name introduced in a palette would be undefined on the default theme, and
  the one name a retheme could not see.
- Restate only what changes. A restatement that resolves to the same value
  with or without its block is inert, and the invariants report it (see
  [Adding a theme](#adding-a-theme), step 4). The base dark block is the one
  block held to completeness: it restates every themeable semantic token, and
  a palette inherits the rest of its chain.
- A theme block declares `color-scheme` exactly when it restates at least one
  colour, and the value is the block's own mode. The browser paints scrollbars,
  form controls and the canvas behind the page from it, so a colourless block
  (a palette that only restates radii) must not claim a mode.

### Translucency knobs and the 80% floor

`tokens-scale.css` carries three theme-invariant knobs that a palette may
turn: `--alpha-chrome` (opacity of the two chrome bars), `--alpha-surface`
(opacity of raised and overlay surfaces: menus, popovers, the modal card, the
drawer, the toast) and `--backdrop-surface` (the whole `backdrop-filter`
value behind a translucent raised surface). Classic holds them at `100%`,
`100%` and `none`, so it paints exactly as it did before the knobs existed: a
colour mixed at 100% is the colour, and `none` -- not `blur(0)`, which still
opens a stacking context -- is the absence of a filter. Each consumer reads
them the same way, one `background: color-mix(in srgb, var(--bg-…)
var(--alpha-surface), transparent)` and one `backdrop-filter:
var(--backdrop-surface)`; only `glass` turns them today.

The two chrome bars take `--alpha-chrome` but never the filter. A non-`none`
`backdrop-filter` makes its element a backdrop root and the containing block
for every fixed descendant: a filtered element nested inside another filtered
element sees only the parent's already blurred paint and loses its own frost,
and a fixed menu inside it stops being fixed to the viewport. The bars host
the floating menus and, below 760px, a fixed app menu, so they stay
filter-free; for the same reason no fixed scrim may be rendered inside a menu,
popover, modal card, drawer or toast.

Every contrast ratio the guardrails prove is measured on the opaque hex a token
resolves to. A surface painted below a point over an unknown ground can drop
below AA in ways no static check can see, so `scripts/style-invariants.test.mjs`
holds every `--alpha-*` knob at or above `80%`, for both knobs; glass sits at
`88%` for the bars, `84%` for light surfaces and `80%` for dark ones.
`prefers-reduced-transparency` is not honoured in this version; the floor is
the substitute.

### Who owns the attributes

`data-theme` is owned by `lib/theme-preference.ts`: the user popover's Theme
group (System, Light, Dark) stores an explicit `light` or `dark` under one
`localStorage` key and removes it for System, and `resolveTheme` folds that
choice together with `matchMedia("(prefers-color-scheme: dark)")` into the
value written to `<html data-theme>`. The media query is read in JavaScript
only: `.stylelintrc.json` keeps `prefers-color-scheme` out of the CSS so that
one place decides the theme and the switch can always override it.

`data-palette` is owned by the same module, with the names in
`lib/palettes.ts`. The administrator's choice lives on the server and rides on
`/api/system/bootstrap`, which a static export cannot read before it paints,
so the value is cached under `PALETTE_STORAGE_KEY` (`open-splunk.palette`),
and the cache is what the pre-paint path reads:

- `app/layout.tsx` inlines `THEME_BOOT_SCRIPT` -- the same resolution as a
  fixed string -- as the first child of `<head>`, ahead of every stylesheet.
  It reads both `localStorage` keys behind their own guards, folds the theme
  as above, and writes `data-palette` from the cache; a cached name this build
  does not ship, or no cache at all, paints `classic`. The unit test holds the
  string and the module functions to one table so they cannot drift.
- `ThemeSync` (`app/_components/theme-sync.tsx`, mounted once from the root
  layout so the sign-in page gets it too) re-applies the cache on mount,
  follows `storage` events for both keys from other tabs, follows the
  operating system's own switch while the preference is System, and -- in
  backend mode only -- fetches `/api/system/bootstrap` once and applies the
  live palette. Bootstrap needs no bearer token, which is what lets the
  sign-in page take the palette; every failure is swallowed, so the demo
  export never asks and an unreachable backend keeps the cached or classic
  palette.
- `applyInstancePalette(palette)` resolves the name (unknown paints classic),
  writes the cache, sets the attribute and updates the browser chrome colour.
  It is idempotent, which is what lets the admin card restore the saved value
  on the way out of a preview without checking whether one was showing.
- `previewPalette(palette)` paints this document only and leaves the cache
  alone: what the Appearance card does while a radio is selected but not yet
  applied. Other tabs follow the cache, so a preview never reaches them, and
  the next boot still paints the server's value.
- `syncThemeColorMeta()` copies the computed `--chrome-bar` into
  `<meta name="theme-color">` after every theme or palette application, so the
  address bar and an installed app's title bar follow the product bar.
  `app/layout.tsx` keeps the classic literal in `themeColor` for the first
  paint only; that is the one colour a TypeScript module may carry.

`integration/style-contracts` pins both attributes' effect on the semantic
tier, the editor, the completion menu and the toast, in every palette and
mode.

### Scales

`tokens-scale.css` owns the named spacing, radius, typography, page-layer,
elevation, motion, opacity, translucency, and font scales. Prefer the nearest
existing semantic or scale token. Do not create a token merely to hide a
one-off literal; first ask whether the component should use an existing
primitive or layout step.

Every step was chosen from the literals the stylesheets shipped, so migrating a
literal onto a step is a substitution. The Replaces tables record which
literal maps to which step and where a substitution deliberately moved a pixel;
a measurement no step names is either recorded in the literal ledger or is a
local stacking value the ladder leaves alone.

Spacing is eight steps on a 4px base. Each literal rounds to its nearest step,
and a tie rounds down so dense surfaces stay dense:

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

Radius has three steps and a capsule; a palette may restate any of them (ocean
softens to `4/10/16px`, glass to `6/12/18px`, graphite to `4/6px`, terminal
squares every corner to `0`):

| Token | Value | Replaces |
| --- | --- | --- |
| `--radius-sm` | 2px | 1px, 2px -- Splunk's near-square corner |
| `--radius-md` | 8px | 7px, 8px, 9px |
| `--radius-lg` | 12px | 10px, 12px |
| `--radius-pill` | 999px | capsules and fully rounded ends |

`border-radius: 50%` stays a literal, and so does `0`: one means "circle" and
the other "no corner", and both depend on the box rather than on this scale.

Elevation is a size ladder (`--shadow-sm` through `--shadow-xl`) plus three
drops named for the surface whose direction no size can express
(`--shadow-drawer`, `--shadow-sheet`, `--shadow-toast`). A palette may restate
a shadow whole -- graphite and terminal turn every drop into a hairline ring --
but its ink stays a literal inside the token layer (the scale file, or the
palette file that restates the shadow), because a primitive has no lightness
step for an alpha and a `color-mix()` cannot appear inside a value the scale
layer owns.

The type ramp is `--type-xxs` (9px) through `--type-xxl` (20px) plus the fluid
`--type-display`. `body` in `base.css` sets `--type-md` (12px); most controls,
tables and chrome set `--type-xs` (10px), which is why the ramp is
bottom-heavy.
`--font-sans`, `--font-mono` and `--font-serif` are the three stacks; terminal
restates `--font-sans` to the mono stack literal, because a semantic token may
not point at another token.

### Deliberate literal exceptions

`scripts/css-literal-debt.json` is compared with the source tree in both
directions. Its colour section is empty. Its scale section records only:

- `border-radius: 50%` when the meaning is explicitly circular;
- the individual geometry/ink parts of a composed `box-shadow`; and
- the twenty-three single-digit `z-index` values that order an element
  inside a stacking context its parent already opened. These are not page
  layers, so `--z-base` would promise an escape they cannot make; the layer
  keeps local stacking ladders literal on purpose, and the exact-token-miss
  sweep excludes `z-index` for that reason.

Adding or removing one of these literals requires updating the ledger. Page
layers use stacking tokens, and complete reusable shadows use elevation tokens.
Named colours remain forbidden everywhere, including token files; palette
primitives use the canonical hex form.

Two colours cannot be reached by a token at all and are still there, each
pinned by the invariants to exactly one site. `app/dashboards/operations-dashboard.css`
carries `fill='%23526068'` inside a `data:` URI select arrow, where the SVG is
its own document, no `var()` resolves and no search for `#` finds it; the fix
when it comes is a mask or an inline SVG whose fill a rule can set, not another
literal. `app/layout.tsx` keeps its `themeColor` literal because the browser
paints from it before any stylesheet loads.

A colour literal that appears outside the token layer is a role gap: a colour
the interface needs that tier 2 does not name. The gaps that have been closed,
and what closed them, are the pattern for the next one:

| Was missing | What closed it |
| --- | --- |
| Alpha of any kind | `color-mix(in srgb, var(--role) N%, transparent)` at the call site. No token carries alpha: a tier-2 `rgb(… / n%)` is a literal, and a tier-1 one would be a primitive with no lightness step. |
| Borders around a status wash | The `--status-*-edge` roles. |
| Ink and surface for dark grounds | `--fg-inverse-muted`, `--accent-inverse`, `--chrome-fg-muted` and `--chrome-fg-faint`; the bars and the inverse surface are two families because the inverse surface flips between themes and the bars do not. |
| Work in flight | `--status-progress`, the warmer caution for running, throttled and quarantined states. |
| Query colouring | The six `--syntax-*` inks, a second categorical family so that reordering a chart's ramp never recolours a query. |
| A second neutral ink step | `--fg-secondary`. |

Record a new literal in the ledger only under a gap like these, and prefer
closing the gap: add a tier-2 token with a role name and a one-line comment,
point it at a primitive, and restate it in the base dark block.

## Breakpoints and interaction

Responsive width rules use only max-width `1240px`, `980px`, `760px`, and
`480px`, in that order after base rules. Two pinned shapes are also allowed:
the coarse-pointer tablet complement at `min-width: 761px`, and the short
viewport guard combining `max-height: 650px` with `max-width: 760px`.

Keep media rules in the stylesheet that owns their base selector. Existing
files use either one responsive appendix or blocks at the end of feature
sections; follow the file's current shape. Within one run of media queries,
a section's media blocks run largest width first, then (pointer: coarse), then
(prefers-reduced-motion: reduce). Every width query is a max-width, so a
narrower one stated first is overridden by the wider one that follows and never
applies; and a tap target set at a width would beat the coarse-pointer minimum
meant to outrank it.

`.stylelintrc.json` pins the widths, so an off-canon breakpoint fails
`npm run lint:css`. The fold table records the steps that were folded onto the
canon and what each fold changed; each row is pinned by a computed-style
contract under "folded breakpoint contracts", mounted at a width inside the
folded band and at one outside it where the fold must have changed nothing:

| Was | Now | Effect |
| --- | --- | --- |
| `1120px` (`analytics.css`) | `980px` | the analytics panel rails stop stacking between 981px and 1120px |
| `800px` (`analytics.css`) | `980px` | the metric grid drops to two columns, and the field list its Example column, from 980px down |
| `430px` (`operations-dashboard.css`) | `480px` | the header actions split two-up and the volume plot shortens from 480px down |
| `420px` (`analytics.css`) | `480px` | the context bar goes single-column and the metric numerals shrink from 480px down |
| `520px` (`workspace-dialogs.css`) | `480px` | the knowledge-inspection definition list stays two-column between 481px and 520px |

Four of the five fold outward, so an adaptation happens at a wider viewport
than before; the `520px` fold is the one that folds inward. The analytics
console's old `650px` step was folded onto `760px` as a deliberate restyle.

## Shared primitives

Use the primitive instead of reproducing its visual declarations in a feature
file:

- Buttons: `.button` with `--primary`, `--secondary`, `--ghost`, `--danger`,
  `--icon`, `--compact`, or `--block`. React code should use the shared button
  component/helper where available.
- Tables: `.table-wrap` and `.table`, with `--fixed`, `--compact`, or `--cards`.
  A feature may define only its column plan and minimum width.
- Status: `.status` and its semantic modifiers.
- Badges: `.badge` and its semantic modifiers.
- Forms: shared field, stack, fieldset, and settings-list structures.
- Overlays: the shared modal/dialog family.
- Product chrome: `ProductShell`, `Wordmark`, and the shared drawer.
- Charts: the shared chart geometry and the semantic chart-series token ramp.
- Skeletons: `.skeleton` with line, width, and block modifiers; decorative nodes stay hidden from assistive technology.

A host may size a shared child through a documented component-scoped custom-
property interface instead of selecting into the child's internals. Add such a
knob beside the component, give it a shipped fallback, and register it in the
structural invariant rather than treating arbitrary custom properties as a new
token lane.

The knobs that exist today:

- `--chart-height`, `--chart-plot-min-height`, `--chart-stroke-width`,
  `--chart-x-axis-height`, `--chart-x-axis-type`, `--chart-y-axis-width` on an
  operations-dashboard panel size the shared line chart it hosts.
- `--pulse-ring-core` on the backend preview pulse composes its ring shadow.
- `--search-editor-max-height` on `.spl-editor` caps how tall the SPL editor
  grows with its query (it is sized by its in-flow highlight mirror and never
  shorter than the two-line 62px composer row). Set it on the container to
  give a host a shorter or taller editor; the two composer buttons keep their
  own height either way.

## Adding a theme

A palette is one CSS file plus the name spelled in the eight places that must
agree on it. The stylesheet never needs to know a new palette exists beyond
its token file; everything else is the guardrails' job.

1. **Add the file and import it.** Create
   `app/styles/tokens-palette-<name>.css` and `@import` it in
   `app/styles/index.css` after `tokens-scale.css`, among the other palette
   files in name order. The file holds exactly two blocks, light then dark,
   and nothing that is not a token (or `color-scheme`).
2. **Add hue steps to tier 1.** Any colour the palette needs that no
   primitive already provides goes into the base `:root` of
   `tokens-color.css`, as `--<hue>-<step>` on the `0`-`950` ladder, darker
   at every step, with a comment naming the palette that uses it. A palette
   file never declares a primitive, and a hue family is never named after a
   palette.
3. **Write the light block** as `:root:where([data-palette="<name>"])`, with
   `color-scheme: light` if it restates any colour. It may restate only names
   the base light block declares, colour and scale alike, and every colour
   restatement carries its own one-line comment.
4. **Restate only what changes.** The light block restates only what differs
   from classic light; the dark block only what differs from the chain
   `[classic light, palette light, classic dark]`. A restatement that resolves
   to the same value with or without its block is dead weight a later theme
   has to keep in step with, and the invariants fail on it. The corollary is
   that a token the palette changes only in light renders in dark exactly as
   classic dark does; if the dark look also needs it, the dark block says so.
5. **Write the dark block** as
   `:root[data-palette="<name>"][data-theme="dark"]`, with
   `color-scheme: dark` if it paints. Remember that classic dark sits between
   the two palette blocks, so a value classic dark already changes may need
   restating here even when the light block set it (terminal's `--chrome-fg`
   is the example).
6. **Spell the name everywhere it is spelled.** The client list is
   `lib/palettes.ts` (`Palette` and `PALETTES`; the invariants read that
   literal, and every parametrised contract iterates it). The wire mapping is
   both maps in `lib/api/ui-palette.ts`; the admin copy is `PALETTE_OPTIONS`
   in `app/admin/appearance-form.ts`. The server side is the Go enum in
   `internal/uipalette/palette.go` (the constant and the `all` list), a
   `UiPalette` value appended to `proto/open_splunk/server_settings_api.proto`
   (never renumbered; regenerate with `make proto` and update the enum pin in
   `contracts_test.go`), the hand-written wire map `uiPaletteWireValues` in
   `internal/server/server_settings_api.go` (both `uiPaletteToProto` and
   `uiPaletteFromProto` read it; the build passes without the entry, and only
   `go test ./internal/server` holds it to `uipalette.All()`), and a new
   SQLite migration that rebuilds the `palette` CHECK on
   `server_appearance_settings` (`0011` is shipped and digest-pinned, so it is
   not edited). Keep all of them in the same order.
7. **Run the guardrails and extend the contracts.** `npm run lint:css` and
   `npm run test:frontend` prove the file shape, the four preludes, the
   `color-scheme` rule, "primitives only in base light", "restate only what
   changes", the role-group collisions, the contrast floor (AA, or the
   palette's entry in `CONTRAST_FLOOR` when it promises more, as graphite
   promises 7:1) and the 80% alpha floor, in all twelve scopes; the
   `lib/api/ui-palette` and `appearance-form` unit tests hold the client
   lists to `PALETTES`; `go test ./...` holds the enum, the CHECK, the wire
   map and the sanitizer to the same list. `npm run test:contracts` runs the palette
   contracts over every name in `PALETTES` automatically; add a dedicated
   contract only for a behaviour peculiar to the new palette, as glass has for
   translucency and terminal for the mono face.

## How to make a visual change

1. Find the semantic role, primitive, or owning feature from the cascade table.
2. Reuse an existing token and primitive. Add a semantic tier-2 role only when
   no existing role describes the intent.
3. Keep responsive behavior beside the base selector and use the canonical
   breakpoint ladder.
4. If the change intentionally alters appearance, add or update a computed-
   style contract for the load-bearing outcome.
5. Run the three guardrail layers below and describe deliberate visual changes
   in the commit body.

## Guardrails — what holds this in place

Run:

```sh
npm run lint:css
npm run test:frontend
npm run test:contracts
```

CI runs every gate the layer is made of: `npm run check:docs`,
`npx --no-install oxlint .`, `npm run lint:css`, `npm run typecheck`,
`npm run test:contracts` and `npm run test:frontend`. These are the checks
that hold the cleanup in place, and `scripts/style-guardrails.test.mjs` fails
if the workflow stops running one of them.

Stylelint rejects colour literals, named/system colours, unapproved font/type,
radius, shadow and stacking values, unsupported media shapes, selector naming
drift, and CSS-module syntax. The token-file override is intentionally narrow;
`color-named` is not exempt.

`scripts/style-invariants.test.mjs` checks token structure and tier use across
every palette and mode, exact literal-ledger equality, the single entry
point/import order, media placement, primitive uniqueness, retired/dynamic
class ledgers, and selector reachability. Its palette section holds
`lib/palettes.ts` and the `tokens-palette-*.css` files to each other, refuses
any prelude but the four above, and measures every mandated text pairing in
all twelve scopes. The reverse markup-to-CSS feature-prefix list is explicit
rather than derived; when adding a feature stylesheet, add its namespace to
that list as part of the same change.

`scripts/style-guardrails.test.mjs` verifies that lint and CI wiring cannot be
silently disabled and covers shorthand/inline spellings that property-based
rules can miss. `scripts/test-frontend.mjs` has a hardcoded unit-test list, so a
new test file must also be registered there.

The Playwright contracts read final values through `getComputedStyle`. They are
for cascade outcomes that lint and source scans cannot prove: viewport folds,
theme resolution, primitive states, chart sizing, modal layers, tap-target
floors, and similar behavior. Install the repository-pinned Chromium runtime
once with `npx --no-install playwright install chromium`. The palette
contracts, in `integration/style-contracts/css-contracts.spec.ts`, are:

| Contract | What it proves |
| --- | --- |
| no element in the shell paints its ink in its own ground | every palette and mode |
| readable text clears its palette's contrast floor on the live page | AA, or 7:1 for graphite, in every palette and mode |
| a keyboard-focused primary button shows a ring that clears 3:1 against its surround | every palette and mode |
| the two chrome bars are distinct from each other and stand off the canvas | every palette and mode |
| glass alone makes the raised surfaces translucent | the alpha and filter knobs reach only glass, and the opaque token still clears the text floor |
| terminal alone sets the mono face on body text and squares every corner | `--font-sans` and the radii |
| a palette light restatement never outranks the base dark block | the `:where()` order dependency |

`node scripts/palette-gallery.mjs` is the eye's check on the contracts: after
`npm run build` it captures the sign-in page, the search workspace and the
admin Server section under every palette in light and dark from the demo
export, and lays them out as one PNG per page under
`test-results/palette-gallery/`. Review a new palette, or a change to the
chrome, a knob or a shared primitive, against it; the palette list comes from
`lib/palettes.ts`, so nothing has to be added for a new name.

Palette work (a new `tokens-palette-*.css`, `lib/palettes.ts`, the boot
script, `ThemeSync`, or the appearance API) has a fourth layer:
`OPEN_SPLUNK_PALETTE_SMOKE=1 go test ./integration -run
'^TestBrowserInstancePaletteSmoke$'` proves the administrator's choice reaches
bootstrap, the cache, the pre-paint boot script and the painted page in a real
browser, without Docker. See `integration/README.md`, section "Instance
palette browser smoke".

The JSON ledgers have distinct roles:

- `css-literal-debt.json` is exact allowed literal debt;
- `css-retired-classes.json` contains the 74 removed classes and their
  replacements; and
- `css-dynamic-classes.json` contains classes that exist only at runtime.

Do not add an exemption, disable a rule, or weaken a contract to make a change
pass. A guardrail failure is evidence that the visual design or the documented
contract needs a deliberate update.
