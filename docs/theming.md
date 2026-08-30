# Theming

How the styling layer is put together, and where to edit it when the product
should look different.

`app/layout.tsx` loads exactly one stylesheet — `app/styles/index.css` — and
that file is nothing but an ordered `@import` list, so the cascade order is a
contract written in CSS rather than one implied by JavaScript import order or
by the bundler's chunking: tokens, base, primitives, features, interaction
floors.

Only the token files are meant to carry a literal. A retheme should be an edit
to `app/styles/tokens-*.css`, not a search across the rules.

That is the state now.
[`app/styles/tokens-color.css`](../app/styles/tokens-color.css) declares every
colour role the product has, `tokens-scale.css` the non-colour scales described
under [Scales](#scales) below, and **no rule outside those two files writes a
colour, a type size, a radius, a shadow, a stacking layer or a font stack as a
literal.** `npm run lint:css` fails on one. It runs inside `npm run lint`, so
`make lint`, `make test` and CI's frontend job all carry it, and CI names it
again as a step of its own.

That went from 1,496 hex literals to none in four phases; the last 245 of them
went in Phase 5, along with 86 font sizes, 12 radii, eight box shadows and the
six off-canon breakpoints. The colour half of `scripts/css-literal-debt.json` is
empty as a result, and the scale half holds only the values a scale cannot
express — a circle, a shadow's geometry, a stacking order local to one
component. The pre-refactor alias block is gone, so there is one name per role.
Recolouring the product is a one-file edit: the structural invariants and
stylesheet lint reject component-level palette primitives and colour literals.

## Where a rule lives

`app/styles/index.css` is the whole load order. `app/globals.css` used to be all
of it — 7,326 lines, with every breakpoint override for every feature collected
into two sections at the foot of the file. Phase 4 carved it up: every rule
moved exactly once and unchanged, each `@media` block moved into the file that
owns its base selectors, and the five CSS modules became plain colocated
stylesheets. A feature and the way it folds are now one edit.

| Order | File | What it owns |
| --- | --- | --- |
| 1 | `app/styles/tokens-color.css` | tier 1 primitives and tier 2 colour roles, plus the dark theme |
| 2 | `app/styles/tokens-scale.css` | every scale that does not vary by theme: space, radius, type, stacking, elevation, motion, fonts |
| 3 | `app/styles/base.css` | the reset, the element defaults, the focus ring, the icon sizes, the shared keyframes |
| 4 | `app/styles/primitives/button.css` | `.button` and its modifiers |
| 5 | `app/styles/primitives/table.css` | `.table-wrap`, `.table`, the fixed/compact modifiers, the card mode |
| 6 | `app/styles/primitives/form.css` | inputs, labels, `.form-stack`, the fieldsets, `.settings-list` |
| 7 | `app/styles/primitives/modal.css` | the dialog shell and the surfaces that only open inside one |
| 8 | `app/styles/primitives/status.css` | `.status` and `.badge` |
| 9 | `app/styles/primitives/layout.css` | the product bar, the app shell, the drawer, the toasts, `.suite-page` / `.suite-card` |
| 10 | `app/search-workspace/search-editor.css` | the search title row, the SPL editor, the time-range popover |
| 11 | `app/search-workspace/search-job.css` | the job strip, the result tabs, the timeline |
| 12 | `app/search-workspace/search-fields.css` | the fields rail and the event list |
| 13 | `app/search-workspace/search-results.css` | patterns, statistics, visualization, the dense result grids |
| 14 | `app/admin/admin.css` | the admin console, Knowledge Manager, Knowledge Preview, Lookup Manager |
| 15 | `app/activity/activity.css` | the activity console |
| 16 | `app/datasets/datasets.css` | the dataset cards and field catalog |
| 17 | `app/signin/signin.css` | the sign-in page |
| 18 | `app/home.css` | the landing page |
| 19 | `app/_components/backend-resource-state.css` | the connected-backend empty and error surfaces |
| 20 | `app/dashboards/dashboards.css` | the operations dashboard's page rules |
| 21 | `app/analytics/analytics.css` | the search-analytics console, prefix `analytics-` |
| 22 | `app/dashboards/operations-dashboard.css` | the operations dashboard and the backend dashboard manager, prefix `operations-` |
| 23 | `app/reports/reports.css` | the reports library and the saved-search console, prefix `reports-` |
| 24 | `app/search-workspace/components/workspace-dialogs.css` | the workspace's dialogs, prefix `workspace-dialog-` |
| 25 | `app/search-workspace/panels/visualization-panel.css` | the column and bar charts, prefix `visualization-` |
| 26 | `app/styles/interaction.css` | the coarse-pointer tap target, the 16px input floor, the reduced-motion cut |

The primitives come before the features on purpose. A feature rule and a
primitive rule regularly have the same specificity — `.reports-table-wrap`
against `.table-wrap`, both one class — and the feature is the one meant to win.
Until Phase 4 the last five files were CSS modules, which the bundler happened
to emit after `globals.css`: the order was real and load-bearing and nothing
wrote it down. Adding a stylesheet is a line in `index.css`, in the position its
rules need.

Four placements are not where the file name would suggest, and each records a
cascade dependency the monolith hid:

- `.search-page` sits in `layout.css` beside `.suite-main`. The search page's
  `<main>` carries both classes, and the shell's `min-height` wins only because
  it is declared later; splitting the pair would hand the win to the page and
  move the fold by seven pixels.
- `.home-hero`, `.home-hero h1` and `.dashboard-title-row` sit in `layout.css`
  with the `.suite-page-heading` family they override, because that family's own
  breakpoint rules override them back.
- the `760px` rule that sets `padding-inline` on the search title row, composer,
  timeline, patterns, statistics and visualization panels sits in
  `search-results.css`, the last of the four files its selectors span. Any
  earlier and a later file's base padding wins it back.
- `interaction.css` is imported last, after every feature. Its three rules list
  selectors from every feature and have to outrank all of them; imported
  earlier, a feature's own `min-height` or `font-size` would win back a tap
  target or an input on a touch device that no desktop screenshot covers.

### One styling lane

**Every stylesheet in the repository is plain CSS with feature-prefixed,
kebab-case class names.** There are no CSS modules, no `:global()`, no
`styles.x` object, and no second way to scope a rule.

A module's scoping came from a build-time hash, and the hash is precisely what
made the rules invisible: nothing could ask whether a module class was still
reachable, nothing could see a module's `.table` as a second implementation of
the `.table` primitive, and nothing could tell a renamed class from a deleted
one. The prefix does the same job in the open: `.reports-table`
cannot collide with `.table` for the same reason `.reports_table_a7f3` could
not, and every check that reads the styling layer can now see it.

The prefix is the feature's directory name, and the classes inside a file all
carry it. Modifiers are BEM — `.reports-table--backend`, `.analytics-severity--high`,
`.visualization-canvas--categorical` — which is what lets a modifier sit beside
its base in the stylesheet and in the class attribute.

`.stylelintrc.json` enforces both halves: `selector-class-pattern` rejects a
camelCase class, and `selector-pseudo-class-no-unknown` no longer exempts
`:global`/`:local`, so writing one is a lint error rather than a scoping tool.
`scripts/style-invariants.test.mjs` fails if a `.module.css` file, a `:global()`
selector or a `styles.x` read comes back.

### Responsive rules live beside their base rules

**Every `@media` block lives in the file that owns its base selectors.** That is
the rule the layer keeps, and it is the one that mattered: the monolith
collected every breakpoint override for every feature into two document-wide
sections, so changing how one feature folded meant editing a file 2,000 lines
away from the rules being folded. A feature and the way it folds are now one
file.

*Where inside that file* is not settled, and the layer ships two shapes:

- **A `/* Responsive */` appendix at the file's foot**, holding every breakpoint
  that file uses, one block per step. Fourteen sheets do this — the `table`,
  `modal` and `layout` primitives, the four `search-workspace` sheets, and
  `admin`, `activity`, `datasets`, `signin`, `home`,
  `backend-resource-state` and `dashboards`.
- **A block at the end of each section**, restating the step for every section
  that needs it. Five sheets do this — `analytics`, `operations-dashboard`,
  `reports`, `workspace-dialogs` and `visualization-panel`, the five that were
  CSS modules until Phase 4 — and they pay for it in restated preludes:
  `analytics.css` opens `(max-width: 760px)` six times and `(max-width: 480px)`
  five times.

The appendix is easier to audit (one place per file lists every width that file
reacts to); the per-section block is easier to read while editing one component.
Neither has been chosen, and nothing enforces one over the other: **follow the
shape of the file you are editing rather than introducing a third.** Reconciling
them is a pure reorder within each file, and the rule-parity check in
`scripts/style-invariants.test.mjs` verifies exactly that, so whichever shape is
eventually picked can be applied without a screenshot moving.

The order of the steps *within* a responsive section is load-bearing wherever
they sit, and is always the same: base rules,
then `1240px`, `980px`, `760px`, `480px`, then `(pointer: coarse)`, then
`(prefers-reduced-motion: reduce)`. The width queries are all `max-width`, so
each overrides the one above it; the pointer query comes after them because a
touch target set at `760px` must not beat the coarse-pointer minimum. Moving a
media block earlier than a base rule that also matches the element silently
inverts an override, which is why the sections are cut where no selector spans
two of them.

Two cross-cutting rules are deliberately not split, and say so where they sit: a
`-webkit-tap-highlight-color` reset that names one link per panel in
`analytics.css`, and a focus ring shared by three controls in
`operations-dashboard.css`. Splitting either would be four copies of one
declaration.

### Reach-ins, and what replaced them

A CSS module could only touch a shared class through `:global(...)`, and
nineteen rules did. That escape hatch reads as a component reaching past its own
markup into someone else's, and it hid the coupling from every check: the
selector was in a file no reachability scan could interpret. All nineteen are
gone, in three different ways, because they were three different problems.

**Two were never reach-ins at all.** `workspace-dialogs.css` styles a capability
control inside the shared `.settings-list`. With one namespace that is an
ordinary descendant selector — `.settings-list label > .workspace-dialog-capability-control` —
and the `:global()` was only ever the module system's tax.

**Thirteen were a component positioning its own legend.** The categorical canvas
laid out `.chart-legend`, which `visualization-panel.tsx` renders itself. Those
rules now sit in `visualization-panel.css` as plain selectors, and the canvas
takes a BEM modifier — `.visualization-canvas--categorical`, beside the existing
`.visualization-canvas--line` — instead of a module class bolted onto a global
one.

**Four were a panel resizing a shared chart**, and those needed an interface
rather than a rename. `operations-dashboard.css` was setting
`.time-series-chart`'s grid tracks, height and stroke width from outside the
component. `.time-series-chart` now reads six custom properties with its shipped
values as fallbacks — `--chart-height`, `--chart-y-axis-width`,
`--chart-plot-min-height`, `--chart-x-axis-height`, `--chart-x-axis-type` and
`--chart-stroke-width` — and the dashboard sets them on its own container, where
they inherit into the chart. The knobs are named and finite; a panel that needs
a seventh has to add it to the component, which is the conversation the reach-in
was avoiding.

### Two dead things Phase 4 removed

Both are invisible in every baseline, and both were only findable once the rules
and the markup were in one namespace.

* `.loadedCountLabel` in the reports console had no call site: two declarations
  and an `8px` font size no element could ever read. Its `font-size` row leaves
  `scripts/css-literal-debt.json` with it.
* `visualization-panel--preview` was the opposite failure — markup asking for a
  class no rule matched, on every live preview. The class is removed from
  `visualization-panel.tsx`, and the new call-site invariant in
  `scripts/style-invariants.test.mjs` is what found it.

## How to restyle X

The load order above is also the search order: start at the top and stop at the
first file that can answer.

**A colour, anywhere.** `app/styles/tokens-color.css`, and nothing else. Change
the tier-2 role (`--accent`, `--status-error`, `--fg-muted`) to move every use
of it; change a tier-1 step to move every role built on it. See
[The rule](#the-rule) and [Role gaps](#role-gaps) for the literals that are not
yet reachable this way.

**Buttons.** `app/styles/primitives/button.css`. `.button` is the one
base; `.button--primary`, `--secondary`, `--ghost`, `--danger`, `--icon` and
`--compact` are its only variants, and `buttonClassName()` in
`app/_components/button.tsx` is the one place markup builds the string. A
feature never restates a button's tone, height or weight — that is how three
button vocabularies grew last time. See [`.button`](#button).

**Tables.** `app/styles/primitives/table.css`.
`.table-wrap` scrolls, `.table` paints, and `.table--fixed`, `--compact` and
`--cards` are the three modifiers. A feature file may add a *column plan* and
nothing else: `.reports-table` is six `th` widths and a `min-width`, and
`.workspace-dialog-history-table` is the same shape. A height, an ink or a
border in a feature table rule is a second table. See [Tables](#tables).

**The product chrome.** `app/styles/primitives/layout.css`, the
`/* Multi-page product shell */` banner, and `app/_components/product-shell.tsx`. Every route renders through
`ProductShell`; a page that needs something in the bar takes a slot rather than
drawing a bar of its own. See [Product chrome](#product-chrome).

**The chart palette.** `--chart-series-1` … `--chart-series-12` in
`tokens-color.css`, assigned in order by `TIME_SERIES_COLORS` in
`app/search-workspace/charts/time-series-line-chart.tsx` — the one array every
chart reads, with `CATEGORY_COLORS` a slice of it rather than a second list.
The four log levels are `--level-debug/-info/-warn/-error` and are not part of
the ramp: a level describes the data, a status describes the outcome of a
search. `scripts/style-invariants.test.mjs` holds both claims.

**A chart's size or stroke.** Not by selecting into it. `.time-series-chart`
reads five custom properties with its shipped values as fallbacks —
`--chart-height`, `--chart-y-axis-width`, `--chart-plot-min-height`,
`--chart-x-axis-height`, `--chart-x-axis-type` — and `.time-series-chart__line`
reads `--chart-stroke-width`. A host panel sets them on its own container and
they inherit: `app/dashboards/operations-dashboard.css` shortens the latency
chart with six declarations and no descendant selector. `.analytics-trend-line`
honours `--chart-stroke-width` too, so the two line charts state one geometry.

**One page.** Its own file, from the table above. Every page has one now; a
surface that grows out of the file it shares takes a file and a prefix of its
own, added to `index.css` in the feature block.

**A breakpoint.** [Breakpoints](#breakpoints). Four `max-width` steps, pinned by
lint; the canon is not a custom property because `@media` is evaluated before
the cascade.

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
the application stylesheets and the CSS modules — and chosen so that every literal the
product used more than twice lands within about 24 RGB units of a step. Nine
hue families survived that exercise: gray, slate, green, blue, amber, orange,
red, purple, and a small set of extended categorical hues (teal, pink, indigo,
brown, olive) that only the chart ramp reaches.

### Tier 2 — the semantic tokens

Names such as `--bg-surface`, `--fg-muted`, `--status-error`, `--chrome-bar`.
Each names the **role** a colour plays, never the colour itself. These are the
only names the application stylesheets, the CSS modules, and component code may use.

The roles are grouped as:

| Group | Tokens |
| --- | --- |
| Backgrounds | `--bg-canvas`, `--bg-surface`, `--bg-subtle`, `--bg-raised`, `--bg-inverse` |
| Foregrounds | `--fg-text`, `--fg-strong`, `--fg-secondary`, `--fg-muted`, `--fg-faint`, `--fg-inverse`, `--fg-inverse-muted`, `--fg-link` |
| Borders | `--border`, `--border-subtle`, `--border-strong`, `--border-focus` |
| Accent | `--accent`, `--accent-hover`, `--accent-bright`, `--accent-inverse`, `--accent-soft`, `--accent-alt`, `--accent-alt-soft` |
| Status | `--status-success`, `--status-info`, `--status-warning`, `--status-error`, `--status-neutral`, `--status-progress`, each with a `-soft` background wash and, except `--status-progress`, an `-edge` border and a `-strong` ink |
| Event severity | `--level-info`, `--level-warn`, `--level-error`, `--level-debug` |
| Charts | `--chart-series-1` through `--chart-series-12`, `--chart-neutral` |
| SPL syntax | `--syntax-pipe`, `--syntax-command`, `--syntax-function`, `--syntax-field`, `--syntax-string`, `--syntax-literal` |
| Chrome | `--chrome-bar`, `--chrome-appbar`, `--chrome-hover`, `--chrome-fg`, `--chrome-fg-muted`, `--chrome-fg-faint` |
| Interaction | `--highlight`, `--selection`, `--focus-ring` |

Phase 5 added the last six groups' newest members, and each one closed a row of
the [Role gaps](#role-gaps) table rather than being invented: `--status-*-edge`
is the mid-tone border between a wash and its solid, which 30 declarations were
spelling by hand; `--status-progress` is the orange the product uses for work
in flight, which tier 1 had two steps of and tier 2 no name for;
`--syntax-*` is the SPL editor's categorical ramp, kept separate from
`--chart-series-*` so that reordering a chart cannot recolour a query;
`--accent-inverse`, `--fg-inverse-muted` and the two `--chrome-fg-*` steps are
the inks that sit on a permanently dark ground.

**Alpha is not a token, it is a `color-mix()`.** A scrim, a focus halo, a
hairline on a chrome bar and a translucent overlay are all a tier-2 colour seen
through what is behind it, and the 53 literals they used to be were the two
grounds mixed by hand. They are now written at the call site as
`color-mix(in srgb, var(--role) N%, transparent)` — `--scrim` and its family
never existed. A token cannot carry the alpha instead: an `rgb(… / n%)` in
tier 2 is a literal, which `scripts/style-invariants.test.mjs` forbids, and one in
tier 1 would be a primitive with no lightness step for the ladder to order. The
mix keeps the theme in charge of the hue, which a literal never did.

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
colour at a contrast ratio of 1.00:1. `scripts/style-invariants.test.mjs` now
checks each foreground against every ground its role comment names, in both
themes.

## The rule

> No literal colour may appear outside `app/styles/tokens-*.css`.

That means no `#rrggbb`, no `rgb()`, `rgba()`, `hsl()`, or `color()`, and no
named CSS colour, anywhere in an application stylesheet, an inline
`style` attribute, or a TypeScript constant that feeds one. `npm run lint:css`
**fails** on one, and is part of `npm run lint`, `make lint` and two steps of
CI's frontend job. The colour half of `scripts/css-literal-debt.json` is empty:
there are no remaining violations to report, which is what turns the sentence
above from an aspiration into a gate.

The same rule now covers six more kinds of value, for the same reason and by
the same mechanism — `font-family`, `font-size`, `font`, `border-radius`,
`box-shadow` and `z-index` must each read a token from
`app/styles/tokens-scale.css`. Three
literals are allowed inside those, and each says something a scale cannot:
`border-radius: 0` and `border-radius: 50%` mean "no corner" and "a circle",
which depend on the box rather than on a step, and a single-digit `z-index`
orders an element inside a stacking context its parent already opened, which is
not a page layer. `!important` is a sixth: it is an error everywhere except the
two files `.stylelintrc.json` names, each with the reason written beside it.

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
- Semantic tokens: `--<group>-<role>`, with the group prefix (`accent`, `bg`,
  `border`, `chart`, `chrome`, `fg`, `level`, `status`) shared by every token in
  it. Modifiers come last: `-hover`, `-bright`, `-strong`, `-soft`, `-subtle`,
  `-alt`. `scripts/style-invariants.test.mjs` holds that list, so a new group is a
  change to this document and to that test together.
- No token name mentions a hue. `--status-error`, never `--status-red`.
- One family, one kind of value. `var(--type-md)` is a length and
  `var(--fg-muted)` is a colour, and the prefix says which before the value is
  looked up. This is why the type scale is `--type-*` and not `--text-*`: the
  names `--text` and `--text-strong` were inks, and a family cannot mean both.
  Both are retired now, but the reason the scale is not called `--text-*` is
  worth keeping.

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
   are checked by `scripts/style-invariants.test.mjs`: a restatement that resolves
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

A theme does not have to restate anything translucent, and cannot. Every scrim,
halo, hairline and overlay in the product is written at its call site as
`color-mix(in srgb, var(--role) N%, transparent)`, so it follows whichever
value the theme gives `--role` and there is nothing for the block to say about
it. That is the reason the alpha family does not exist: a stored
`rgb(16 23 28 / 48%)` would be a fourth thing every theme had to keep in step,
and the mix already has the answer.

## Retired aliases

The pre-refactor names — `--muted`, `--surface`, `--green-strong`, and their
peers — were declared at the bottom of the light block through Phase 1, each
pointing at its semantic replacement so no call site broke while the layer was
introduced. Phase 2 rewrote the last call site and **deleted the block**, along
with the `--shadow` declaration `app/globals.css` carried. There is now exactly
one name per role.

| Retired | Now read as |
| --- | --- |
| `--app-bar` | `--chrome-appbar` |
| `--app-bar-hover` | `--chrome-hover` |
| `--black` | `--bg-inverse` |
| `--blue` | `--status-info` |
| `--blue-soft` | `--status-info-soft` |
| `--border-dark` | `--border-strong` |
| `--canvas` | `--bg-canvas` |
| `--faint` | `--fg-faint` |
| `--green` | `--accent` |
| `--green-soft` | `--accent-soft` |
| `--green-strong` | `--accent-hover` |
| `--muted` | `--fg-muted` |
| `--orange` | *(named no role; `--orange-400` is unreferenced tier 1)* |
| `--product-bar` | `--chrome-bar` |
| `--red` | `--status-error` |
| `--red-soft` | `--status-error-soft` |
| `--shadow` | `--shadow-lg` |
| `--surface` | `--bg-surface` |
| `--surface-raised` | `--bg-raised` |
| `--surface-subtle` | `--bg-subtle` |
| `--text` | `--fg-text` |
| `--text-strong` | `--fg-strong` |
| `--yellow` | *(named no role; `--amber-500` is unreferenced tier 1)* |

`--border` is not in the table: it survived the refactor as a tier-2 name
rather than as an alias. The value each retired name carried is pinned against
the role that carries it now in `scripts/style-invariants.test.mjs` and
`integration/style-contracts/css-contracts.spec.ts`. A separate assertion
holds that none of the names comes back.

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
alone appears 170 times and moves to 12px. Migrate it surface by surface, run
the structural invariants and computed-style contracts, and never do it in one
pass.

### Radius

| Token | Value | Replaces |
| --- | --- | --- |
| `--radius-sm` | 2px | 1px, 2px — Splunk's near-square corner, and already the most common of the thirteen distinct radii in use, at 24 declarations |
| `--radius-md` | 8px | 7px, 8px, 9px |
| `--radius-lg` | 12px | 10px, 12px |
| `--radius-pill` | 999px | capsules and fully rounded ends |

`border-radius: 50%` stays a literal, and so does `0`. One means "circle" and
the other "no corner"; both depend on the box rather than on this scale, so
neither is a step. `.stylelintrc.json` allows exactly those two beside a
`var(--radius-*)`, in any corner position, so `var(--radius-sm) var(--radius-sm)
0 0` is a legal top-rounded box and `1px 1px 0 0` is not.

Phase 5 migrated the twelve declarations that were still off the scale: the
seven at 1px, the two at 7px, the two at 9px and the one at 10px, plus the two
corner lists that spelled 1px twice. Every one moved by at most 2px.

### Type

The product's body size is 10px, so the ramp is deliberately bottom-heavy and
`--type-xs`, not `--type-md`, is the default UI size. Seven steps cover the
twenty distinct `font-size` literals in use.

The family is `--type-*` rather than the `--text-*` these steps were first
written as, because `--text` and `--text-strong` were colours: with both
families in the layer, `var(--text-…)` told a reader nothing about whether they
were writing an ink or a length. Both inks are retired now — see
[Retired aliases](#retired-aliases) — but the scale keeps the `--type-*` name,
because it has 588 call sites and the collision it was avoiding could recur the
moment someone reached for `--text-` again.

| Token | Value | Replaces | Declarations covered |
| --- | --- | --- | --- |
| `--type-xxs` | 9px | 7px, 8px, 9px | 130 |
| `--type-xs` | 10px | 10px | 344 |
| `--type-sm` | 11px | 11px | 60 |
| `--type-md` | 12px | 12px, 13px | 38 |
| `--type-lg` | 14px | 14px, 15px | 25 |
| `--type-xl` | 16px | 16px, 17px | 34 |
| `--type-xxl` | 20px | 18px through 25px, 28px | 38 |

The one `clamp(32px, 4vw, 55px)` on the sign-in headline is `--type-display`.
It is not a step on this ramp — it is a fluid size, and its own token for that
reason — but Phase 5's lint allows no `font-size` literal outside the token
layer, and a headline that resizes with the viewport is exactly the kind of
value a retheme should be able to reach. `scripts/style-invariants.test.mjs` reads
a `clamp()` over lengths as a length, so the `--type-*` family still holds one
kind of value.

Phase 5 migrated the 86 declarations that were still off the ramp. Two folds
lost their point in the process and were re-aimed rather than left: the
analytics metric numeral was 21px falling to 18px at 480px and the page heading
23px falling to 21px, and both ends of both rounded to `--type-xxl`, so each
narrow-viewport rule became a restatement of the rule it was overriding. They
now step to `--type-xl`, which keeps the fold the fold this ramp can express.
`integration/style-contracts/css-contracts.spec.ts` asserts the analytics one
at 450px and at 500px.

### Stacking

Nine steps replace the twenty-nine distinct `z-index` values the application
stylesheets ship across fifty-five declarations, ordered

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

`.skip-link` is `--z-skip-link`, the tenth rung, and keeps the 1000 it shipped.
It has to be reachable above every layer including a toast, and it was the
ladder's documented literal exception until Phase 5's lint stopped allowing a
`z-index` literal to be a page layer — at which point "above every step" was
easier to state as a step than as a comment.

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
- `.field-inspector`. It is rendered inside `.fields-rail`, which opens a
  stacking context, so its number never reaches the page: it means "above the
  rail's own contents", and the highest of those is `.fields-topbar` at 3. It
  read 40 on the desktop and 110 on mobile; the mobile one was deleted as
  redundant, and Phase 5 brought the desktop one to 4, which says the same
  thing inside the same context and is what `.stylelintrc.json` allows a local
  lift to be.

The five sheets that were CSS modules until Phase 4 declare ten more, all
between 1 and 8 and all inside their own component's stacking context. They stay
literal for the same reason.

The lint draws the line at a single digit: `.stylelintrc.json` allows `z-index`
to be `var(--z-*)` or `0`-`9` and nothing else. A local order inside one
component has never needed more than four steps, and a number wider than a digit
is a claim about the page — which is what the ladder is for.

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

**A bar's popover is not on `--z-dropdown` — the bar is.** "Every such popover
is on `--z-dropdown`, above both" is true of the declaration and false of the
render. `.suite-product-bar` is `position: sticky` with a `z-index`, which makes
it a stacking context, so a `.suite-popover` or a `.floating-menu` inside it is
painted at the bar's layer no matter what its own `z-index` says. Every dismiss
scrim is a step above every bar by design, so the scrim covered every item of
every open bar menu: the menus looked and read exactly as before, and no pointer
could reach one. Keyboard navigation still worked, which is why nothing noticed.

The fix is the move `.search-title-row:has(.floating-menu)` already makes one
layer down — raise the ancestor, not the popover:

```css
.suite-product-bar:has(.floating-menu),
.suite-product-bar:has(.suite-popover) {
  z-index: var(--z-dropdown);
}
```

The bar joins `--z-dropdown` only while it holds an open menu, so the ladder's
"scrim above bar" ordering still holds at rest, and the raise covers the app bar
overlapping the popover's top 34px as well. Both spellings are listed because
the shell paints `.suite-popover` and the search workspace passes its own
`.floating-menu` markup into the same bar slots.

### Elevation

| Token | Value | Replaces |
| --- | --- | --- |
| `--shadow-sm` | `0 1px 4px rgb(21 36 45 / 9%)` | the 1–2px ambient shadows on cards and rows, and the statistics scroll hint's `0 2px 7px rgb(20 35 44 / 16%)` |
| `--shadow-md` | `0 3px 9px rgb(21 35 43 / 24%)` | the 3–7px lift on menus and popovers, the analytics tooltip's `0 3px 9px rgb(18 29 35 / 25%)`, and the resource menu's `0 7px 18px rgb(18 29 36 / 18%)` |
| `--shadow-lg` | `0 10px 30px rgb(18 29 36 / 18%), 0 2px 7px rgb(18 29 36 / 12%)` | the retired `--shadow`, and nothing else |
| `--shadow-xl` | `0 18px 55px rgb(10 20 26 / 28%)` | `.modal-card` |
| `--shadow-toast` | `0 8px 28px rgb(12 22 28 / 28%)` | `.toast` |
| `--shadow-drawer` | `8px 0 30px rgb(18 29 36 / 24%)` | `.drawer` |
| `--shadow-sheet` | `0 -12px 36px rgb(18 29 36 / 24%)` | the time-range popover, as a phone sheet |

`--shadow-lg` is byte-identical to the `--shadow` it replaced, so the seven
rules that read the outgoing name did not move when it was retired.

**A shadow token replaces a shadow only when the geometry matches exactly.** An
elevation is an offset, a blur and an ink, and swapping one for "the nearest
token" changes the shape of the shadow rather than its hue — which is a
different kind of change from everything else in this phase, and the one thing
a hue-normalisation sweep may not do. Two rules were migrated onto `--shadow-lg`
during Phase 2 and moved back:

| Rule | Literal | `--shadow-lg` would have made it |
| --- | --- | --- |
| `.modal-card` | `0 18px 55px rgb(10 20 26 / 28%)` | y 18px → 10px, blur 55px → 30px, alpha 28% → 18%, plus a second stop |
| `.toast` | `0 8px 28px rgb(12 22 28 / 28%)` | y 8px → 10px, blur 28px → 30px, alpha 28% → 18%, plus a second stop |

Phase 5 closed that gap, because its lint allows no box-shadow ink outside the
token layer and a literal drop had nowhere left to live. Four of the eight got a
name of their own — `--shadow-xl` for the modal, and `--shadow-toast`,
`--shadow-drawer` and `--shadow-sheet` for the three whose shadow travels
sideways or upwards, which no rung of a size ladder can stand in for. Two
collapsed onto the ladder, and both are geometry changes rather than hue ones,
so they are listed here rather than assumed:

| Rule | Was | Now | Effect |
| --- | --- | --- | --- |
| `.resource-action-menu` | `0 7px 18px rgb(18 29 36 / 18%)` | `--shadow-md` | y 7px → 3px, blur 18px → 9px, alpha 18% → 24%: a tighter, slightly darker lift, and the same one every other menu in the product draws |
| `.statistics-scroll-hint` | `0 2px 7px rgb(20 35 44 / 16%)` | `--shadow-sm` | y 2px → 1px, blur 7px → 4px, alpha 16% → 9%: the ambient shadow every other row-level surface draws |

The eighth, `.event-field-actions`' `-8px 0 7px var(--bg-surface)`, was never a
shadow: it is the surface colour painted sideways to fade a row under a sticky
action column, its ink is already a token, and it keeps its geometry.

The shadow inks are still literals in `tokens-scale.css`, and that is the one
piece of this debt Phase 5 did not pay. A shadow's ink is a colour and its
geometry is a scale; splitting the two would put `--shadow-*` in one file and
`--shadow-ink-*` in another, and `scripts/style-invariants.test.mjs` requires a
semantic token to point at a tier-1 primitive, which an `rgb(… / n%)` cannot be
— a primitive is `--<hue>-<step>` and there is no lightness step for an alpha.
Every other translucent value in the product went to `color-mix()` for that
reason; a `box-shadow` cannot, because the mix would have to appear inside a
value the scale layer owns.

### Focus

`--focus-ring` is a colour, so it is declared in `tokens-color.css` with the
rest of tier 2 and not here; a theme has to be able to move it. It currently
resolves to `--blue-450` (`#2f8ac1`), the literal the primary focus outline
already uses, so nothing shifts today.

The focus blues are down to one. `#317fa6`, `#3b88b5`, `#3b83a6` and
`rgb(47 120 158)` no longer appear on any focus state; every one of them now
reads `--focus-ring` or `--border-focus`. The alpha outline
`rgb(42 120 158 / 28%)` was the exception until Phase 5 and is now
`color-mix(in srgb, var(--focus-ring) 28%, transparent)`, along with the other
sixteen translucent blues the product drew as rings, glows, selections and
scroll fades. They ran from 5% to 45% and from `rgb(39 116 155)` to
`rgb(58 136 181)`; they are three steps now — 38%, 26% and 10% — over one hue
that a theme can move. `integration/style-contracts/css-contracts.spec.ts` pins the
outline's computed colour, which Chromium serializes as
`color(srgb 0.184314 0.541176 0.756863 / 0.28)`.

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
off-canon breakpoint **fails** `npm run lint:css`. Six off-canon widths survived
Phase 3 — 1120px, 800px, 650px, 520px, 430px and 420px, plus one
`max-height: 650px`. Phase 4 folded five of them onto the canon; Phase 5 folded
the sixth as a deliberate restyle:

| Was | Now | Effect |
| --- | --- | --- |
| `1120px` (`analytics.css`) | `980px` | the analytics panel rails stop stacking between 981px and 1120px; both columns fit that range comfortably |
| `800px` (`analytics.css`) | `980px` | the metric grid drops to two columns, and the field list drops its Example column, from 980px down instead of from 800px down |
| `430px` (`operations-dashboard.css`) | `480px` | the header actions split two-up and the volume plot shortens from 480px down |
| `420px` (`analytics.css`) | `480px` | the context bar goes single-column and the metric numerals shrink from 480px down |
| `520px` (`workspace-dialogs.css`) | `480px` | the knowledge-inspection definition list stays two-column between 481px and 520px |

Four of the five fold *outward*, so an adaptation happens at a wider viewport
than before and no width is left cramped-but-unadapted; the `520px` fold is the
one that folds inward, and it costs a definition list one column across a 40px
band inside a dialog.

Each row is pinned by a computed-style contract under "folded breakpoint
contracts" in `integration/style-contracts/css-contracts.spec.ts`.
Each mounts the named surface at a width inside the folded band and asserts the
promised layout, plus one width outside it where the fold must have changed
nothing. Reverting any of the five steps to its pre-fold value turns those red.

`650px` is folded onto `760px`, which finishes the canon. It was the analytics
console's mobile step, so the fold is a deliberate restyle rather than a
rename: the console adapts from 760px down where it used to adapt from 650px down.
`analytics.css` stated the step six times because each section carries its own
responsive rules, so this is six edits of one breakpoint rather than six
different ones.

The one `max-height: 650px` compound is untouched and is stated **twice**: the
monolith held a single `(max-height: 650px) and (max-width: 760px)` block
covering the time-range popover and the sign-in card, and those belong to two
different features, so the split gave each its own copy — `app/signin/signin.css`
and `app/search-workspace/search-editor.css`. Phase 5 put `max-height` on the
allowed feature list and pinned its one value, because it is not a width step
and cannot fold onto the canon: it asks whether the viewport is short enough
that a sheet has to give height back, which no `max-width` expresses. Pinning
the value is what keeps it from becoming a second ladder. The two guards can
still only be folded into one if the sign-in card and the popover can share a
step, which is a design question rather than a rename.

## Primitives

A token layer only makes a retheme a one-file edit if there is also one rule
that reads each token. Three families in the application stylesheet had grown a copy per
feature instead, so a colour change had to be repeated by hand and the copies
drifted apart in ways no token could reconcile. Phase 3 collapsed them.

Each family below is the **only** implementation of its idea. A feature may add
a class beside one of them, but only for layout the feature genuinely owns — a
width, a grid, a `display` toggle. Restating a tone, a height or a weight in a
feature class is the thing that produced the copies in the first place.

### `.button`

One class plus BEM modifiers, in `app/styles/primitives/button.css`. `app/_components/button.tsx` composes the same classes from props and
is worth using where the variant is *computed* — the search workspace's run
button is accent while a search can be started and destructive while one can be
cancelled, and it is the call site the component exists for. Where the variant
is a constant, `className="button button--primary"` says the same thing with one
fewer indirection, and both spellings are in the tree deliberately.

| Modifier | Meaning |
| --- | --- |
| *(none)* | the quiet default: surface ground, `--border-strong` edge |
| `--primary` | the accent action; one per surface |
| `--secondary` | a filled control on a surface that is already white |
| `--danger` | a destructive action |
| `--ghost` | borderless; shows a ground only on hover |
| `--icon` | square and padding-free, for a control whose label is one glyph |
| `--compact` | one scale step shorter, for a control inside a dense row |
| `--block` | stretches to its container, for a stacked mobile action |

Sizes read `--space-7` and `--type-xs` rather than literals, so a button's
height and label size move with the scale in `app/styles/tokens-scale.css`.

**Replaces.** `.button` (with `.primary`/`.secondary`/`.danger`/`.compact`),
`.suite-button` (with `--primary`, plus a `--secondary` that one call site used
and no rule ever defined), `.icon-button` and `.close-button`. Kept beside it
for layout only: `.run-button` (a 62px two-line block welded to the SPL editor,
whose tone now comes from `--primary`/`--danger` rather than from its own
`.cancel` class),
`.time-range-button` (a 62px three-column grid), `.activity-filter-button` (a
tab with an underline, not a button),
`.suite-user-button` (inverse ink on the product bar), `.mobile-fields-button`
(a `display` toggle with no paint of its own), `.sampling-button`, `.row-overflow`
and `.table-action`.

### `.status`

One family for every outcome indicator, replacing eight parallel spellings of
the same six ideas: `.status-icon`, `.status-dot`, `.status-label`,
`.mini-status`, `.job-state-icon`, `.job-card-state`, `.inspector-state`, and
the reports module's own `.status`/`.runStatus`.

Shape and tone are separate modifiers, so a tone is one declaration rather than
one per shape: `.status--dot` (a 7px disc), `.status--icon` (a 20px disc around
a glyph) and `.status--label` (a row of text that contains a dot) all read the
same `.status--success`, `--info`, `--warning`, `--error`, `--neutral` and
`--running`. `--running` is `--info` plus the pulse, because an in-flight state
is informational and spelling it as two classes invited half the call sites to
forget the second one.

The tone always sits on the element that paints. `.status--label` therefore
carries no tone of its own — the swatch inside it does — which is why
`StatusLabel` and `StatusDot` in `app/_components/status.tsx` exist: the pairing
is easy to get wrong by hand, and the wrong version still renders.
`StatusIcon` in `app/_components/app-icon.tsx` reads the same vocabulary.

**Not folded in: `.history-state`.** The search-history table still paints its
outcome by tinting the label text (`.history-completed`, `.history-failed`,
`.history-canceled`), which is the behaviour the "Job-card and inspector state"
row below says was retired. The row is about the job cards, and those are
migrated — including the four history cards in the Jobs dialog, whose call site
outlived its rules for one commit. `.history-state` is a ninth spelling and a
tenth surface; it is recorded here so the contradiction is a known debt rather
than a doc that does not match the tree.
`scripts/style-invariants.test.mjs` now fails when a class the consolidation
retired is still built into a `className`, in an attribute or through an
interpolation, so a rule cannot be deleted out from under a call site again.

### `.badge`

One non-interactive label chip: a pill of uppercase `--type-xs`, with tone the
only thing a caller decides (`--success`, `--info`, `--warning`, `--error`,
`--neutral`, and `--outline` for a chip that has to read as a chip on a ground
its own colour already matches).

**Replaces.** `.mode-pill`, `.role-pill`, `.severity-badge`, the two
byte-identical `previewBadge` rules in `analytics.css` and
`operations-dashboard.css`, `.readOnlyBadge`, `.liveBadge`/`.partialBadge`
and `.availableBadge`/`.unavailableBadge` — eight, which is the number the
Badges banner in `app/styles/primitives/status.css` states.

**Not folded in: `.demo-badge`.** It is a ninth chip and it is still its own
implementation — a 21px pill with a `10px` radius rather than `--radius-pill`,
its own border and padding, and an `i` swatch the primitive has no slot for. It
shares its shell rule with `.unsaved-dot` and `.search-draft-hint`, which are
not chips at all, so folding it needs those two untangled first. Recorded here
rather than fixed because it is a fourth surface's worth of work, and an
undocumented exception is how a primitive quietly acquires a tenth copy.

### Field validation

One appearance for a form field the product cannot accept, in
`app/styles/primitives/form.css`, and one pairing that produces it, in
`app/_components/field-validation.tsx`.

There was no primitive here before, and the result was a defect rather than
merely a duplication. Five forms marked an invalid control with `aria-invalid`
— search limits, the reports rename and duplicate dialogs, the
index-observability range, the sign-in token field and the knowledge-manager
advanced filters — and exactly two of them painted it: `.signin-card` and
`.knowledge-manager__advanced-filter-grid` each carried a private
`border-color: var(--status-error)`. In the other three the attribute was set
and nothing anywhere selected it, so a value the form would refuse rendered
identically to one it would accept, and the only signal was a submit button
greyed out without naming a field. The search-limits form even computed a
per-field message and rendered it in `--fg-muted`, the same ink as the hint it
replaced.

| Part | What it is |
| --- | --- |
| `[aria-invalid="true"]` | the invalid field's `--status-error` edge |
| `.field-error` | the note under it: `--status-error`, with a warning glyph |
| `fieldControlProps(id, error)` | `aria-invalid` and `aria-describedby` for the control |
| `FieldNote` | the `<small>` the control points at: hint, or error |

**Both selectors are doubled**, and this is the one place in the layer besides
`.table--cards` where that is the right tool. `.settings-form-grid input` is
(0,1,1) and states a `border` shorthand; `.settings-form-grid label > small` is
(0,1,2) and states a muted colour; every feature stylesheet loads after the
primitives. A single-attribute or single-class rule here would be set and then
painted over by the very rules it exists to override.

**Keying on the attribute, not on a class**, is deliberate: a form earns the
appearance by marking the field the way a screen reader needs it marked, so the
two cannot come apart. `integration/style-contracts/css-contracts.spec.ts` pins
both halves against the four grids that state their own field borders.

**The note is not a live region.** These fields validate on every keystroke, and
`role="alert"` would interrupt a screen reader with a half-typed value's
complaint on every character. A form that needs to announce something on submit
keeps its own status region — the knowledge-manager filters and the token
dialogs each have one — and the per-field pairing stays quiet until focus.

**Not folded in: the block-level error.** `.reports-action-error` and
`.backend-inline-error` are a red wash across the width of a dialog, not a note
under one field, and one of them covers two fields at once. They are a second
shape rather than a second implementation, and both now sit above fields the
primitive paints.

### Pixels this deliberately moved

Unifying three implementations means at most one of them keeps its pixels. Each
row below records the appearance that was deliberately chosen.

| What | Was | Now | Why |
| --- | --- | --- | --- |
| Button height | 34px (`.button`), 31px (`.suite-button`), 29px (`.compact`) | `--space-7` (32px), `--space-7 - --space-1` (28px) | one row height, taken off the scale rather than from either copy |
| Button label | 13px (`.button`), 10px (`.suite-button`) | `--type-xs` (10px) | `--type-xs` is the default UI size the token layer documents, and the size the rest of the chrome already used |
| Button padding | `0 15px` (`.button`), `0 12px` (`.suite-button`) | `--space-3` (12px), `--space-2` (8px) compact | on the spacing scale |
| Button weight | 700 (`.button`), 600 (`.suite-button`) | 700 | the heavier of the two; a button reads as an action |
| `.suite-button:hover` edge | darkened to `#8f9ca3` | unchanged edge, ground moves only | one hover behaviour, and one fewer literal |
| `.button--danger` edge | `#a93e38` | `--status-error-strong` | a role, not a nearby colour |
| `.button--secondary` ink | `#44535c` | `--fg-secondary` | the role that literal was approximating |
| Status dot | 8px (`.status-dot`, `+5px` right margin), 10px (`.mini-status`), 7px (`.status-label i`) | 7px, no margin | one disc; the gap now belongs to the row that lays the dot out |
| Status icon | 20px (`.status-icon`), 18px (`.job-state-icon`) | 20px | one disc |
| `.mini-status.state-success` | `#69a23e` | `--status-success` | the success role, not a second green |
| Job-card and inspector state | tone painted the label text | tone paints the swatch, text is `--fg-muted` | matches every other status label in the product |
| Badge radius | `10px` | `--radius-pill` | on the radius scale; indistinguishable at chip height |
| `.liveBadge`, `.partialBadge`, `.severity-badge` | mixed case | uppercase | one chip shape |
| `.mode-pill--demo` ink | `#865e00` | `--status-warning-strong` | a role, not a nearby colour |
| `.history-clear-button` edge | `#caa6a3` | the standard control edge | the destructive intent stays in the label ink |
| `.run-button.cancel` edge | `#983832` | `--status-error-strong`, via `.button--danger` | the run button's two states are now the primitive's two tones |
| Snapshot-bar buttons | `.live-jobs-snapshot button`, a bare descendant rule restating the primitive | `.button.button--compact` | one implementation; the accent ink stays as a one-line feature override |
| Reports table status ink | `reports.css` `.status`/`.runStatus` set no colour and inherited the cell's `--fg-secondary` | `--fg-muted`, from `.status--label` | the status vocabulary owns the label ink; the outcome is carried by the swatch, not by the text being a shade darker than its neighbours |

**Residual `button` debt.** 195 rules in the application stylesheets still style a bare
`button` as a descendant of a feature (`.search-actions > button`,
`.pagination button`, and so on) rather than through `.button`. They are not
copies of the primitive — most set only a height, a gap or an ink for a control
that is already inside a laid-out row — but they are the surface a tenth button
variant would grow from, and each one is a place a theme change has to be made
twice. `.live-jobs-snapshot button` was the one that genuinely restated the
primitive and is the row above; the rest are recorded here as debt rather than
migrated, because reaching them means touching every feature block in the file.

## Consolidated primitives — tables, chrome and overlays

A token layer only makes a theme editable if each primitive has one
implementation to point it at. Where the stylesheet carried the same widget
two or three times, the copies have been folded into one and the consumers
migrated. Consolidation is not free: two copies that had drifted apart cannot
both survive, so each fold below records the appearance that was chosen and
the one that was given up.

### Animations

| One keyframe | Replaced | Deliberate change |
| --- | --- | --- |
| `spin` | `app-icon-spin`, `spinner`, `backend-state-spin` | none — all three were `to { transform: rotate(360deg); }` |
| `pulse-ring` | `pulse`, `status-pulse`, `backend-preview-pulse` | the running status dot (`.status--running`) used to hold a steady 4px glow at mid-cycle; it now emits the same expanding, fading halo as the other two. The backend-preview dot's halo grows to 4px instead of 5px and fades from 30% rather than 15% |

`pulse-ring` paints an optional inner ring from `--pulse-ring-core`, which
defaults to a fully transparent shadow. `.backend-preview-status__pulse`
declares that property once and reads it both for its resting `box-shadow` and,
through the keyframe, for its animated one — so the solid 1px ring that used to
force a second copy of the keyframe is now a value rather than a rule.

### Tables

`app/styles/primitives/table.css` carries one table family and every table in
the product is built from it:

| Class | What it is |
| --- | --- |
| `.table-wrap` | the horizontal scroll container (was `.responsive-table-wrap`) |
| `.table` | the primitive: header ground, cell borders, row hover, link and code inks (was `.product-table`) |
| `.table--fixed` | `table-layout: fixed`, for a table that declares column widths on its header |
| `.table--compact` | shorter rows for a dialog, where a row is a line of text rather than a record |
| `.table--cards` | below 760px each row becomes a card and each labelled cell prints its column name |

Three implementations went into it — `.product-table`, the reports console's own
`.table`, and the search-history dialog's `.historyTable` — and the two feature
stylesheets keep only their column plans, as `.reports-table` in
`app/reports/reports.css` and `.workspace-dialog-history-table` in
`app/search-workspace/components/workspace-dialogs.css`. Deliberate visual
changes:

* **Report rows are shorter.** The reports table declared a 67px row against
  the product's 45px. It now uses 45px; the report cell is three lines tall, so
  the rendered row settles around 55px rather than 67px.
* **The search-history dialog uses the product header.** Its header was 34px of
  uppercase 9px text on the same grey; it is now the product's 10px
  sentence-case header, and its rows are `--compact` at 36px rather than 49px.
  Its cells also move from `--fg-muted` to the product's `--fg-secondary` ink
  and from a top border per row to the product's bottom border, which removes
  the doubled line under the header.
* **Card labels are one style.** Four copies of the `attr(data-label)` label
  existed — under `.live-jobs-table`, under `.historyTable`, under the reports
  table, and under `.knowledge-manager__row`. They disagreed on ink
  (`--fg-faint` vs `--fg-muted`), size (10px vs 9px) and tracking (none vs
  0.04em vs 0.06em). The survivor is `--fg-muted`, 9px, 0.06em, uppercase, so
  the report cards' labels are smaller and tracked where they were larger and
  plain.
* **Only labelled cells stack.** A card cell with a `data-label` puts the label
  above the value in a column; a cell without one — a favourite toggle, an
  action row, a `colspan` empty state — keeps the default row flow. This is what
  lets one card rule serve a table whose first column is a star and a table
  whose first column is a title.

`.table--cards` doubles its own class in every selector. The desktop column
widths it has to beat (`.live-jobs-table td:first-child { width: 30% }`) and a
feature's own `min-width` are each one class, and a card that lost to either
would keep a horizontal scroll bar or a 30%-wide first column.

The activity console's own card mode is gone with it. `.mobileCardTable` in
`app/activity/activity-console.module.css` was a fifth complete card-table
design — sixteen rules, one of them a byte-for-byte restatement of the
seven-declaration visually-hidden `thead` rule above — laying its label *beside*
the value in a 72px column rather than above it. It was briefly kept as "a
different design rather than a drifted copy", but the same page renders
`.table--cards` in backend mode from `backend-activity-console.tsx`, so the
argument only established that one page had two answers to the question. The
demo console now opts into `.table--cards` like its backend twin, and the module
file is deleted. Its mobile layout changed: labels sit above values in a
two-column card, the search cell is the card's title and the action row spans
the foot.

The wrap around a card table is part of the same primitive:
`.table-wrap:has(> .table--cards)` drops the scroll-hint gradient and the
`overflow-x` clip, because a card table is exactly as wide as its wrap and has
nothing to scroll to. That replaces `.live-jobs-table-wrap` and
`.mobileCardTableWrap`, which each stated those two declarations for one page,
and it reaches the two card tables that had neither.

### Product chrome

The search workspace hand-rolled a second product header beside `ProductShell`:
`.product-bar` / `.app-bar` / `.product-menu-button` / `.product-utilities` /
`.global-search` / `.user-button` / `.app-tabs` / `.app-identity`, parallel to
`.suite-product-bar` / `.suite-app-bar` and their utilities. The two had drifted
— 36px against 38px on the product bar, 43px against 34px on the app bar, 13px
against 10px tab labels — so "the product header" had two answers to every
question a theme asks.

`app/search-workspace.tsx` now renders `ProductShell`, which grew optional
slots for what a page's own header needs: `appSwitcher` (a page that switches
apps in place rather than by navigating; supplying it also stops the shell
making a bootstrap request of its own), `utilities`, `overlays` for modals and
scrims that must sit outside `<main>`, `disclosure`, `mainClassName`/`mainId`,
`onSignOut`, and the shell's own class and test hooks. The whole
`.product-bar` family is deleted.

Deliberate visual changes on `/search/`:

* the product bar is 38px rather than 36px and sticks to the top of the page;
* the app bar is 34px rather than 43px, its tab labels are `--type-xs` rather
  than 13px, and an active tab is marked by the suite's filled ground and
  underline rather than a 3px green bar;
* the app identity block is 190px wide with a 22px glyph rather than 222px with
  a 28px one;
* below 760px the app bar is hidden, as it already was on every other page. The
  app switcher is **not**: hiding the shell's own switcher there is safe because
  the drawer lists the same apps underneath it, and a page that supplies its own
  switcher gets the drawer's single-app branch instead. Folding that page's
  switcher away too would leave the one page that changes app in place with no
  way to change app at all, which the shell's mobile rule reads as
  `.suite-product-bar > .suite-catalog-anchor` so it names only the shell's own
  anchor. The switcher keeps the pre-shell header's mobile width, `195px` and
  `calc(100vw - 142px)` below 480px, with the app name ellipsised;
* the drawer is the shell's, so its identity block reads `Local session` /
  `Single-user backend mode` in backend mode where the page's own drawer always
  read `Administrator` / `admin@localhost`; it gains the shell's help label and
  rule above sign-out, and its two aria labels are the shell's
  ("Mobile product navigation", "Close navigation");
* the skip link reads "Skip to main content" rather than "Skip to search
  workspace";
* Ctrl/⌘K focuses the page's own find input. The shell binds the shortcut and
  the page replaces the shell's `utilities` slot, so the shell resolves the
  field from the bar when its own slot is not rendered, rather than calling
  `preventDefault()` and focusing nothing.

### Wordmark

One component, `app/_components/wordmark.tsx`, and one `.wordmark` block. The
markup was written out four times and styled by three blocks — `.wordmark`,
`.suite-wordmark`, and `.signin-wordmark`/`.signin-mobile-brand` — which
disagreed on both inks. The bar sizing is the base, because three of the four
call sites sit in a bar; `.wordmark--hero` is the sign-in brand. The sign-in
mark's ink moves from `#abb6bc`/`#78b44a` to the bar's `#aeb9bf`/`#79b84a`.

### Drawer

`.suite-mobile-drawer`, `.search-mobile-drawer` and the three identical
scrims (`.suite-mobile-backdrop`, `.search-mobile-backdrop`,
`.time-picker-mobile-backdrop`) become `.drawer` and `.drawer-backdrop`, with
`.drawer-trigger`, `.drawer-label`, `.drawer-rule`, `.drawer-app-state` and
`.drawer-app-retry` for its parts. The two drawers differed only in an
inherited ink and in whether a nav icon was a `<span>` or an `<i>`; the search
page now opens the shell's drawer, so its copy is gone rather than reconciled.

### Modal

`Modal` lives in `app/_components/modal.tsx` beside the `modal-surface.ts` it
uses, instead of under `app/search-workspace/` with six importers reaching
across from `app/admin`, `app/activity` and `app/reports`. Its CSS family is
unchanged apart from a duplicate set of mobile `.modal-card` overrides, which
sat at the same 760px breakpoint as the full-screen ones that shadowed them.

## What keeps one primitive from becoming two again

Nothing in the toolchain reports the two failures a consolidation can have.
CSS has no duplicate-rule error, so a second `.button` base compiles and one of
the two silently wins on file order; and an unmatched class is not an error
anywhere, so markup left behind by a deleted rule renders with no styling and
raises nothing. Four source checks stand in for the errors the language does
not have.

`scripts/style-invariants.test.mjs` asserts that each primitive — `.button`,
`.table`, `.table-wrap`, `.status`, `.badge`, `.wordmark`, `.drawer`,
`.drawer-backdrop`, `.modal-card` — has exactly one unconditional base rule; a
responsive or theme override under an at-rule is not a second base, because it
can only apply where the base already does. The same file asserts that `spin`
and `pulse-ring` are each declared once, that the six keyframe blocks they
replaced stay gone, and that every `animation` names a block that exists — a
dangling animation name renders as no animation at all, in silence.

It also asserts that no two rules state the same four or more declarations.
`scripts/css-duplicate-blocks.json` records the restatements this phase
deliberately left, each with the primitive that would otherwise own it. An
entry is keyed by both the declarations and the exact set of rules stating
them, so it goes stale — and the test fails — the moment either side moves.
That is what stops the record drifting into a blanket exemption. The largest
entries name the next consolidation: the knowledge- and lookup-manager form
controls (four copies of one input rule), the uppercase field label (three),
and muted body copy (four).

The same file walks the other direction — from every call site back to the
styling layer, which is the direction a deletion gets wrong.
`scripts/css-retired-classes.json` lists the sixty-seven global classes Phase 3
deleted and the primitive that replaced each, and nothing in the repository may
write one into a `className` or build one from an interpolation base. The same
file pins the class-coupled selectors in
`integration/browser_vertical.spec.ts`: that spec is driven by the Go
end-to-end harness against a compiled build, so a class it can no longer find
surfaces as a timeout minutes into a run, far from the rename that caused it.

It also asserts that every feature-prefixed class the markup writes is defined
by some stylesheet. That check replaces the one Phase 4 made impossible: a CSS
module resolved to a plain object, so reading a class it no longer had put the
string `"undefined"` into the class attribute, and the invariant compared each
`styles.x` read against its module. With plain strings that failure is gone and
so, without a replacement, was the coverage — a missed call site in a rename now
renders an unstyled element and raises nothing. The scan is keyed on the five
feature prefixes (`analytics-`, `operations-`, `reports-`, `visualization-`,
`workspace-dialog-`) and reads class attributes only, so an element id or a
module name that shares a prefix is never mistaken for a class. It found
`visualization-panel--preview` on the first run.

## Guardrails — what holds this in place

Almost nothing on this page is visible to the toolchain by default. CSS reports
no duplicate rule, no unmatched class, no dangling `var()` and no missing
keyframe block, and a lint count falls when a rule is deleted exactly as it
falls when a rule is migrated. Three
gates stand in for the errors the language does not have.

| Gate | Command | What it can see |
| --- | --- | --- |
| Structural invariants | `npm run test:frontend` | everything on this page that is a property of the source: token structure, naming, literals, cascade order, one-of-each-primitive, reachability |
| Stylesheet lint | `npm run lint:css` | the per-declaration rules stylelint can express, at **error** severity — colour, type, radius, shadow and stacking policy, the breakpoint canon, the class-name pattern, the `:global`/`:local` ban. `.stylelintrc.json` is the policy, and [What the lint enforces](#what-the-lint-enforces) says what it exempts and why |
| Computed-style contracts | `npm run test:contracts` | what a rule resolves to in a browser at eight widths |

`make test` runs all three, and CI runs them in its Ubuntu `frontend` job.

### What the lint enforces

Everything above is a decision. This section is the list of decisions a machine
now keeps, so that none of them depends on a reviewer remembering this file.

`npm run lint:css` runs stylelint over `app/**/*.css` at **error** severity. It
is the tail of `npm run lint`, which `make lint`, `make test` and CI's frontend
job all run, and CI names it again as its own step so a stylesheet failure is a
red line that says "stylesheets" rather than the end of a step that says
"oxlint".

| Rule | What it stops |
| --- | --- |
| `color-no-hex`, `color-named`, `declaration-property-value-disallowed-list` | A hex, `rgb()`, `rgba()`, `hsl()` or `hsla()` anywhere but the token files; a named colour such as `white` or `red`, at the top level or inside a `linear-gradient()`; and a system-colour keyword such as `Highlight` or `ButtonFace`. A translucent value is a `color-mix()` over a tier-2 token; an opaque one is the token. |
| `declaration-property-value-allowed-list` | A literal `font-family`, `font-size`, `border-radius` (the shorthand and the four `border-*-radius` longhands), `box-shadow` or `z-index`. Each must read a token, with three documented exceptions: `border-radius: 0` and `50%`, and a single-digit `z-index` for a local stacking order. The `font` shorthand is pinned to `inherit`, because a rule keyed on `font-size` and `font-family` never sees the size and face that `font: 12px/1.4 system-ui` states; longhands are the only way to write either. `box-shadow`'s whole-value `var()` must name the `--shadow-` or `--pulse-ring-` family, so an ink token cannot stand where a shadow belongs. |
| `media-feature-name-allowed-list`, `media-feature-name-value-allowed-list` | A breakpoint off the four-width canon, the one `min-width: 761px` touch guard and the one `max-height: 650px` short-viewport guard. |
| `declaration-no-important` | An `!important` outside the two files the config names, each with the reason written beside it. |
| `selector-max-specificity` (`0,4,2`) | A selector deeper than the shared table primitive's deliberately doubled card-mode rules, which are the ceiling the layer needs. |
| `selector-class-pattern`, `selector-pseudo-class-no-unknown` | A class outside kebab-case or BEM with a feature prefix, and the CSS-module scoping escapes that are silent no-ops in a plain stylesheet. |
| `stylelint-config-standard` | Unknown properties, duplicate selectors, deprecated properties and keywords, redundant longhands. |

Two `overrides` entries carry the exemptions, and both are scoped to named
files with the reason in the entry: `app/styles/tokens-*.css` is exempt from the
value rules because the token layer is where a value belongs, and
`app/styles/interaction.css` and `app/reports/reports.css` are exempt from
`declaration-no-important`. There are no `stylelint-disable` comments anywhere
in the tree, and adding one would be the wrong shape of fix: an exemption that
travels with a line is an exemption nobody reviews.

### The invariant suite

`scripts/style-invariants.test.mjs` is the whole of the first gate: one file,
a hundred and two tests, run by `npm run test:frontend`. It replaced seven files that
asked overlapping questions through three parsing libraries — two of which
exported a function of the same name meaning different things. All the reading
and parsing now lives in `scripts/style-inventory.mjs`, so the test file never
opens a stylesheet, which is the first thing it asserts about itself.

The sections, in the order they appear in the file:

1. **Reach.** One test asserts that every walk the rest of the file depends on
   is populated — the stylesheets, the test files, the token layer, the import
   list, the frozen rule set, the call sites. Every other assertion compares one
   list against another and goes quiet when either side is empty, so this is the
   test that stops the file passing by having nothing to look at.
2. **The token layer.** One declaration site per name; every reference inside
   the layer resolving inside it; no colour literal in a token that names
   another token; a dark block that redefines only names the light block
   declares; nothing outside `tokens-color.css` reading a tier-1 primitive; and
   no stylesheet outside `app/styles/tokens-*.css` declaring a token of its own,
   bar the seven component knobs listed in the test.
3. **The naming grammar.** Every name parses under [Naming](#naming); no
   semantic name mentions a hue; a step number really does say how light a
   primitive is (measured in CIE L\*); a name family holds one kind of value; a
   semantic token points at a primitive and a primitive holds a literal; two
   roles in one group never resolve to the same colour; the dark theme restates
   everything it must and nothing it need not; and every text pairing a token's
   own role comment promises keeps WCAG AA in both themes.
4. **The literal sweep.** The colour and scale literals outside the token layer
   must equal `scripts/css-literal-debt.json` exactly — a new literal fails, and
   so does a ledger row whose literal has been migrated. Separately: no literal
   that a token already spells exactly, no colour hidden in a percent-encoded
   `data:` URI beyond the one recorded site, no colour literal in TypeScript
   beyond the browser theme colour, and every token TypeScript writes into the
   DOM declared by the layer.
5. **The stylesheet set.** `app/styles/index.css` imports every application
   stylesheet exactly once; `app/layout.tsx` is the only file that pulls a
   stylesheet in; the entry point declares no rule of its own; no `.module.css`,
   `:global()`, `:local()`, `composes` or `styles.x` read comes back; no test
   file reads a stylesheet's characters, however the path is composed; and the
   load order is tokens, base, the six primitives, the features, then
   `interaction.css` last.
6. **Parity.** `scripts/css-phase3-monolith.json` freezes the rule set the
   single application stylesheet stated at the commit before it was split. Every
   rule is still stated once by the split set, unchanged, and every repeated
   selector keeps the order that decides its value.

   This is a ratchet on about twenty stylesheets, so **how to write a new rule
   in one of them is part of the gate**, not a way around it. The ledger's
   `substitutions` array records every rule the split set states that the
   monolith did not state verbatim, and it has three forms:

   | `before` | Means |
   | --- | --- |
   | one rule | that rule was edited into `after` |
   | a list of rules | those rules were folded into the single `after` |
   | `[]` | `after` is a rule nobody wrote before |

   The empty list is the addition form: write the rule, run
   `npm run test:frontend`, and the failure prints the rule's signature
   verbatim — `<at-rules> \|\| <selector list> \|\| <declarations>` — which is
   what goes in `after`. An addition is compared as the last declaration of its
   selector, so if it ties on specificity with a rule already there and the
   cascade does not in fact reach it last, the tie-break check fails too; that
   is a real finding about the rule, not about the ledger.

   Do not delete the parity test to land a rule. The ledger is a provenance
   proof rather than a standing budget, and its own header says when to retire
   it: when a phase has rewritten so much of the frozen text that there is
   nothing left to check against, delete the file and the test together, on
   purpose, rather than regenerating either to pass.
7. **Where a responsive rule lives.** No `@media` block overrides base rules
   that live in another file, `interaction.css` still earns its exemption from
   that check, and every run of media queries follows the documented order.
8. **One implementation of each primitive**, plus the shared animations and the
   duplicate-declaration check described under
   [What keeps one primitive from becoming two again](#what-keeps-one-primitive-from-becoming-two-again).
9. **Reachability, in both directions — but not symmetrically.** Rule to
   markup is total: every class rule in every global stylesheet must be
   produced by some literal `className`, interpolation base or selector, or be
   recorded in `scripts/css-dynamic-classes.json`. Markup to rule is scoped to
   the five namespaces the colocated feature stylesheets own — `analytics-`,
   `operations-`, `reports-`, `visualization-` and `workspace-dialog-` — which
   is the whole of `FEATURE_PREFIXES` in `scripts/style-invariants.test.mjs`.
   A class outside those five that no rule defines is **not** reported: the
   scan cannot tell an unstyled `home-page` from a word that was never meant to
   be a class, and guessing in either direction would make it useless. So an
   unstyled `search-…`, `home-…`, `admin-…`, `activity-…` or `suite-…` class
   renders unstyled with nothing to say so, and adding a sixth colocated
   stylesheet means adding its prefix to that list by hand.
10. **The parsers underneath**, pinned against the shapes that have already
    fooled a simpler implementation — an escaped quote, a nested template, a
    commented-out rule, a value carrying its own commas and colons, a path
    composed inside a `readFile` call.

### The ledgers

Five JSON files record what the invariants deliberately allow. Each is compared
against the tree in **both** directions, which is the property that makes them
ratchets rather than exemption lists: an entry whose subject has been fixed
fails the suite just as loudly as an unrecorded violation.

| File | Records |
| --- | --- |
| `scripts/css-literal-debt.json` | every colour and scale literal left outside the token layer |
| `scripts/css-retired-classes.json` | the sixty-seven classes the consolidation deleted, each with its replacement |
| `scripts/css-duplicate-blocks.json` | the restatements left in place, each with the primitive that would otherwise own it |
| `scripts/css-dynamic-classes.json` | classes that only ever reach the DOM at runtime |
| `scripts/css-phase3-monolith.json` | the frozen pre-split rule set, and the edits the split and the lint gate did not make verbatim |

Adding a row is therefore a deliberate act with a reviewer reading it, and
paying one off is a deletion the suite demands rather than one nobody notices.

## Known debt in the token layer

`.stylelintrc.json` carries an `overrides` entry exempting
`app/styles/tokens-*.css` from `color-no-hex`, `color-named`, the `rgb()`
disallow-list and the six value allow-lists: those rules exist to push every literal into the token
layer, so firing them there would ask the palette to point at itself and the
rules Phase 5 flipped to errors could never be satisfied. The exemption is
scoped to the two token files and to the rules that name a value — everything
else still applies there, and `scripts/style-invariants.test.mjs` polices what a
token file may contain far more
closely than a stylesheet linter can. That scoping is what makes "no literal
outside `app/styles/tokens-*.css`" a machine-checkable rule rather than a
convention.

A second `overrides` entry names the two files that may still carry
`!important` — `app/styles/interaction.css` and `app/reports/reports.css` — and
says why beside each. The reduced-motion block has to beat every animation any
rule declares, including ones added later by a file it has never heard of, and
`*` is the lowest specificity there is. The reports card-mode overrides fight
`.table--cards.table--cards td[data-label]:first-child`, which is (0,4,1)
because the shared table primitive doubles its class on purpose; a report cell
would have to restate five class-level selectors to reach it, and raising the
feature that far would make the primitive impossible to change without changing
every feature that shadows it. Phase 5 took the other eleven declarations off
`!important` — 25 down to 14 — by raising a selector where one was genuinely
losing and by deleting the rest, which were fighting nothing.

The shadow tokens in `tokens-scale.css` are the same tension seen from the
scale side: their geometry is a scale but their ink is a colour. Phase 5 tried
to split them and could not — see [Elevation](#elevation) for why an alpha
cannot be a tier-1 primitive — so they still hold the literals the stylesheets
ship today, and a shadow that changes colour still changes every elevated
surface at once.

Three text pairings are short of WCAG AA (4.5:1) and are inherited rather than
introduced: `--fg-faint` on `--bg-surface` is 3.10:1 in the light theme and
3.90:1 in the dark, and `--fg-link` on `--bg-canvas` is 4.47:1 — three
hundredths short. `scripts/style-invariants.test.mjs` checks only the pairings a
token's own role comment promises, so these are not test failures; moving them
is a palette change with call sites behind it, which belongs to a migration
phase rather than to this one.

**The sweep introduced three more and they are fixed rather than recorded.** A
substitution that picks the nearest *fill* role for an ink lightens it, and a
13px bold glyph on its own wash has no margin to spend: the five connected-
backend state badges went to `--status-neutral`, `--status-warning` and
`--status-info` and landed at 3.61:1, 3.88:1 and 4.29:1. They now read
`--fg-secondary` and the `--status-*-strong` family, and
`integration/style-contracts/css-contracts.spec.ts` measures all five against their own
computed grounds, so the next sweep cannot repeat it. The remaining ink moves of
20 units or more all lighten onto `--fg-muted`, which is 4.93:1 on
`--bg-surface` and 4.44:1 on `--bg-subtle`; the second of those is the
`--fg-faint` problem one step up the ramp and is recorded here rather than
fixed, because it is the same palette decision.

The four uncollapsed focus blues are gone, and so is the alpha outline
`rgb(42 120 158 / 28%)` that outlived them. Alpha was the shape of the whole
remaining colour debt — 53 of the 245 literals Phase 5 cleared were an
`rgb(r g b / n%)`: the mobile scrims, the white bar hairlines, the selection
wash, the focus halos, the translucent overlays. The answer was `color-mix()`
at the call site rather than a token, and it is a better answer than a token
would have been: `color-mix(in srgb, var(--bg-inverse) 48%, transparent)` says
what a scrim *is*, and a theme that moves `--bg-inverse` moves the scrim with
it, which a stored `rgb(16 23 28 / 48%)` never could. Only the shadow inks are
left, for the reason above.

**The alias block is deleted**, along with the `--shadow` declaration in
`app/globals.css`; see [Retired aliases](#retired-aliases) for the mapping and
for the three test tables that were retargeted at the roles rather than dropped.
The check is worth re-running rather than trusting:
`grep -rnE 'var\(\s*--(surface|text|muted|canvas|shadow)\s*[,)]' app lib integration`.

### Colour mappings that move a pixel

The `Replaces` columns under [Scales](#scales) call out every substitution that
deliberately changes a rendered value. The colour half needs the same list, and
it is longer than the token-definition table it started as. Most substitutions
round near-duplicate greys onto one step. The moves worth knowing about,
largest distance first:

| Distance | Substitution | Where |
| --- | --- | --- |
| 55 | `#dfa024` → `--status-warning` `#a87300` | warning icons and text; the largest move in the phase, 244px |
| 39 | `#a23e39` → `--status-error` `#c93c37` | the failed-run status dot |
| 38 | `#d9e0e3` → `--chrome-fg` `#ffffff` | the mobile hamburger bars, three 17×1px rules |
| 36 | `#63a33c` → `--accent` `#477f2b` | accent fills, 1,510px across nine pages |
| 33 | `#68a63f` → `--accent` | the same family, one step lighter |
| 29 | `#ac3d37` → `--status-error-strong` `#8f2f2b` | the error state badge, darkened deliberately (see below) |
| 27 | `#fff3ce` → `--status-warning-soft` `#fff8e9` | the demo mode pill |
| 25 | `#466239` → `--status-success-strong` `#376a20` | the two success action notices |
| 23 | `#345d71` → `--fg-secondary` `#43525a` | the sign-in help notice |
| 18 | `#79ad55` → `--accent-bright` `#69a343` | the timeline columns, 72 per search page |
| 17 | `#4f6c7b` → `--fg-secondary` | the neutral state badge, darkened to clear AA |

Distance is the largest single-channel difference. Everything above 24 is a
place where no role-correct token was closer, so the choice was between a wrong
role and a kept literal; each row above chose the role and is listed here for
that reason. The token-definition mismatches that were already recorded stay
true and are small:

| Token | Resolves to | Replaces | Distance |
| --- | --- | --- | --- |
| `--chart-series-1` | `--green-600` `#5f9c3a` | `TIME_SERIES_COLORS[0]` `#5f9f3a` | 3 |
| `--chart-series-2` | `--blue-600` `#2878a8` | `TIME_SERIES_COLORS[1]` `#2f7fa6` | 7 |
| `--chart-series-3` | `--amber-400` `#dda229` | `TIME_SERIES_COLORS[2]` `#e49a2c` | 8 |
| `--chart-series-5` | `--red-400` `#c84f48` | `TIME_SERIES_COLORS[4]` `#c6534c` | 4 |
| `--chrome-bar` | `--slate-900` `#1e252b` | `.suite-product-bar` `#1d252b` | 1 |
| `--chrome-appbar` | `--slate-700` `#3f464c` | `.suite-app-bar` `#424a50` | 4 |

`TIME_SERIES_COLORS` lives in
`app/search-workspace/charts/time-series-line-chart.tsx`; series 4 and 6
through 12 already match it exactly.

**Four kinds of substitution were reverted or re-aimed** after review, because
each picked a token of the wrong role rather than a token that was merely far
away:

- **Faint dividers.** 76 declarations whose literal sat in the `#e0e4e6`–
  `#e7eaec` band were collapsed onto `--border` `#cfd4d7`, a 17–24 unit
  darkening across nine pages and the largest-area visible change in the phase.
  They now read `--border-subtle` `--gray-200` `#e6eaec`, at most 6 units from
  the literal each replaced. The 18 declarations in the `#dce1e3`–`#dfe4e6` band
  stay on `--border`: they are nearer `--gray-250`, and splitting the divider
  role three ways would be a design decision rather than a substitution.
- **Green washes.** Nine success and selected grounds (hue 93–109°, saturation
  0.31–0.44) were mapped onto neutral `--bg-subtle`/`--bg-canvas`/`--bg-raised`,
  which dropped the tint to saturation 0.04 and put them beyond the reach of a
  retheme of `--accent-soft`. They now read `--accent-soft` when they mean
  "selected" and `--status-success-soft` when they mean "succeeded".
- **Chart-ramp fills.** Four non-chart surfaces — the coverage and duration
  progress fills, the dashboard volume bars, and the "good" sparkline — read
  `--chart-series-1`, so reordering the categorical ramp would have recoloured
  them. They now read `--accent-bright`. `--chart-series-*` is for series marks
  and legend swatches only.
- **Two role mistakes.** `.lookup-manager__preview > header` used the background
  wash `--status-info-soft` as a border and now uses `--border`;
  `.token-reveal code` used `--accent-soft` as an ink on `--bg-inverse` and now
  uses `--fg-inverse`. A white panel and its buttons
  (`.backend-resource-state`) had gone to `--status-neutral-soft` when
  `--bg-surface` matched them exactly, and are back on `--bg-surface`.

The categorical ramp has a second, older problem it inherits from
`TIME_SERIES_COLORS` and does not fix: its bottom end is thin. Pairwise CIE76
ΔE puts `--chart-series-6` and `--chart-series-10` 10.4 apart,
`--chart-series-1` and `--chart-series-12` 12.8, and `--chart-series-2` and
`--chart-series-8` 16.4, where a categorical palette usually wants 20 or more.
A twelve-series legend therefore has two pairs that read as one colour.
Widening the ramp is a design change rather than a refactor, so it is recorded
here rather than made quietly.

## Role gaps

**The table this section held is empty, and the section is kept for the shape
of the argument rather than for the rows.** The sweep left 196 hex literals and
49 `rgb()` calls — down from 1,496 before it — and they were never a residue to
grind down one by one: they clustered into roles that tier 2 did not name, and
the fix for each was a token, after which its call sites collapsed together.
Phase 5 named the last of them, and the colour half of
`scripts/css-literal-debt.json` is now `{}`.

What each gap became:

| Was missing | Count | What closed it |
| --- | --- | --- |
| Alpha of any kind | 53 | `color-mix(in srgb, var(--role) N%, transparent)` at the call site. No token carries alpha and none should: a tier-2 token holding an `rgb(… / n%)` is a literal, and a tier-1 one would be a primitive with no lightness step for the ladder to order. The mix keeps the hue themeable, which is what the gap was really about. |
| `--status-*-edge` | ~30 | The four `-edge` roles, pointing at `--red-300`, `--amber-300`, `--blue-300` and `--green-300`, which tier 1 already named. `--status-neutral-edge` joined them for symmetry. |
| Ink and surface for dark grounds | ~14 | `--fg-inverse-muted`, `--accent-inverse`, and `--chrome-fg-muted` / `--chrome-fg-faint`. The bars and the sign-in story panel are dark in both themes and the inverse surface flips, so these are two families rather than one, for the same reason `--chrome-fg` and `--fg-inverse` are. |
| Orange | ~8 | `--status-progress`, the warmer caution the product uses for work in flight — running, throttled, quarantined. Tier 1 had `--orange-400` and `--orange-500` and gained `--orange-600` for the one ink that needed a darker step. |
| A `--syntax-*` family | 7 | Six roles named after what the parser found: `--syntax-pipe`, `--syntax-command`, `--syntax-function`, `--syntax-field`, `--syntax-string`, `--syntax-literal`. Deliberately not `--chart-series-*`: reordering a chart's ramp must not recolour a query. |
| `--shadow-xl` | 9 | `--shadow-xl` plus `--shadow-toast`, `--shadow-drawer` and `--shadow-sheet` for the three drops whose direction no size step can express, and two collapses onto `--shadow-md` and `--shadow-sm`. See [Elevation](#elevation). |
| A second neutral ink step | ~6 | `--fg-secondary` absorbed the cluster. The literals around `--gray-650` were 3 to 14 units from `--gray-700`, close enough that a second step would have been a distinction nothing rendered. |

**Two literals cannot be reached by a token at all and are still there.**
`app/dashboards/operations-dashboard.css` carries `fill='%23526068'` inside a
`data:` URI select arrow, where no `var()` resolves and no grep for `#` finds
it — it wants a mask or an inline SVG. `app/layout.tsx` keeps its `themeColor`
literal deliberately, because the browser paints from it before any stylesheet
loads; `scripts/style-invariants.test.mjs` allows exactly that one string and
fails on any other TypeScript colour.

**What replaced the table is the lint.** A role gap used to be discovered by
reading this document; it is now discovered by `npm run lint:css` failing on
the literal somebody wrote because no role fitted. The gap is the same, the
report is immediate, and the fix has not changed: add a tier-2 token with a
role name and a one-line comment, restate it in the dark block, and point it at
a primitive.
