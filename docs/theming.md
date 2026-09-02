# Theming

Open Splunk has one plain-CSS cascade. `app/layout.tsx` imports only
`app/styles/index.css`; that file imports every stylesheet exactly once in a
deliberate order. Do not add CSS imports to components.

Colours, typography, scale radii, page-level stacking, complete shadows,
motion, and fonts come from `app/styles/tokens-color.css` and
`app/styles/tokens-scale.css`. Component styles still contain ordinary layout
geometry and spacing. A checked literal ledger permits only the intentional
scale exceptions described below; it is a ratchet, not a general exemption.

## Where a rule lives

Shared elements belong in `app/styles/base.css` or a primitive under
`app/styles/primitives/`. A feature rule belongs in the `*.css` file beside the
feature. Responsive rules stay with their base selectors. The complete cascade
is:

| Order | File | Ownership |
| ---: | --- | --- |
| 1 | `app/styles/tokens-color.css` | primitive palette, semantic colour roles, dark-theme role overrides |
| 2 | `app/styles/tokens-scale.css` | spacing, radii, type, stacking, elevation, motion, opacity, fonts |
| 3 | `app/styles/base.css` | reset, element defaults, focus ring, shared keyframes |
| 4 | `app/styles/primitives/button.css` | `.button` and modifiers |
| 5 | `app/styles/primitives/table.css` | `.table-wrap`, `.table`, modifiers and card mode |
| 6 | `app/styles/primitives/form.css` | fields, labels, form and settings layouts |
| 7 | `app/styles/primitives/modal.css` | dialog shell and modal-only surfaces |
| 8 | `app/styles/primitives/status.css` | `.status` and `.badge` |
| 9 | `app/styles/primitives/layout.css` | product shell, drawer, toasts and suite layouts |
| 10 | `app/styles/primitives/chart.css` | shared chart geometry |
| 11 | `app/search-workspace/search-editor.css` | search title, editor and time picker |
| 12 | `app/search-workspace/search-job.css` | job strip, tabs and timeline |
| 13 | `app/search-workspace/search-fields.css` | fields rail and events |
| 14 | `app/search-workspace/search-results.css` | patterns, statistics and result grids |
| 15 | `app/admin/admin.css` | administration and knowledge surfaces |
| 16 | `app/activity/activity.css` | activity console |
| 17 | `app/datasets/datasets.css` | dataset cards and field catalog |
| 18 | `app/signin/signin.css` | sign-in page |
| 19 | `app/home.css` | landing page |
| 20 | `app/_components/backend-resource-state.css` | connected-backend empty/error state |
| 21 | `app/dashboards/dashboards.css` | dashboard page rules |
| 22 | `app/analytics/analytics.css` | analytics console; `analytics-` namespace |
| 23 | `app/dashboards/operations-dashboard.css` | operations dashboard; `operations-` namespace |
| 24 | `app/reports/reports.css` | reports and saved searches; `reports-` namespace |
| 25 | `app/reports/alerts.css` | alert management; `alerts-` namespace |
| 26 | `app/search-workspace/components/workspace-dialogs.css` | workspace dialogs; `workspace-dialog-` namespace |
| 27 | `app/search-workspace/panels/visualization-panel.css` | result visualizations; `visualization-` namespace |
| 28 | `app/styles/interaction.css` | coarse-pointer and reduced-motion floors |

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

`tokens-color.css` has two tiers:

- Tier 1 is the raw palette. Only the token file reads these hue/lightness
  names.
- Tier 2 names semantic roles such as foreground, surface, border, accent,
  status, syntax, chart-series, and chrome colours. All other stylesheets read
  only these roles.

To recolour an existing role, change its tier-2 mapping. Add a tier-2 token only
when the role is genuinely new, with a one-line role comment. Never reference a
tier-1 palette token from a component.

The `:root[data-theme="dark"]` block restates tier 2 only. The attribute is
owned by `lib/theme-preference.ts`: the user popover's Theme group (System,
Light, Dark) stores an explicit `light` or `dark` under one `localStorage` key
and removes it for System, and `resolveTheme` folds that choice together with
`matchMedia("(prefers-color-scheme: dark)")` into the value written to
`<html data-theme>`. `app/layout.tsx` inlines `THEME_BOOT_SCRIPT` -- the same
resolution as a fixed string -- as the first child of `<head>`, ahead of every
stylesheet, so a static-export page paints in the right theme on first load;
`ThemeSync` then follows the operating system's own switch while the
preference is System and picks up a choice made in another tab. The media
query is read in JavaScript only: `.stylelintrc.json` keeps
`prefers-color-scheme` out of the CSS so that one place decides the theme and
the switch can always override it. `integration/style-contracts` pins the
attribute's effect on the semantic tier, the editor, the completion menu and
the toast.

`tokens-scale.css` owns the named spacing, radius, typography, page-layer,
elevation, motion, opacity, and font scales. Prefer the nearest existing semantic or
scale token. Do not create a token merely to hide a one-off literal; first ask
whether the component should use an existing primitive or layout step.

### Deliberate literal exceptions

`scripts/css-literal-debt.json` is compared with the source tree in both
directions. Its colour section is empty. Its scale section records only:

- `border-radius: 50%` when the meaning is explicitly circular;
- the individual geometry/ink parts of a composed `box-shadow`; and
- small component-local z-index values inside an already-created stacking
  context.

Adding or removing one of these literals requires updating the ledger. Page
layers use stacking tokens, and complete reusable shadows use elevation tokens.
Named colours remain forbidden everywhere, including token files; palette
primitives use the canonical hex form.

## Breakpoints and interaction

Responsive width rules use only max-width `1240px`, `980px`, `760px`, and
`480px`, in that order after base rules. Two pinned shapes are also allowed:
the coarse-pointer tablet complement at `min-width: 761px`, and the short
viewport guard combining `max-height: 650px` with `max-width: 760px`.

Keep media rules in the stylesheet that owns their base selector. Existing
files use either one responsive appendix or blocks at the end of feature
sections; follow the file's current shape. Put coarse-pointer rules after width
rules and reduced-motion rules last.

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

Stylelint rejects colour literals, named/system colours, unapproved font/type,
radius, shadow and stacking values, unsupported media shapes, selector naming
drift, and CSS-module syntax. The token-file override is intentionally narrow;
`color-named` is not exempt.

`scripts/style-invariants.test.mjs` checks token structure and tier use, exact
literal-ledger equality, the single entry point/import order, media placement,
primitive uniqueness, retired/dynamic class ledgers, and selector reachability.
The reverse markup-to-CSS feature-prefix list is explicit rather than derived;
when adding a feature stylesheet, add its namespace to that list as part of the
same change.

`scripts/style-guardrails.test.mjs` verifies that lint and CI wiring cannot be
silently disabled and covers shorthand/inline spellings that property-based
rules can miss. `scripts/test-frontend.mjs` has a hardcoded unit-test list, so a
new test file must also be registered there.

The Playwright contracts read final values through `getComputedStyle`. They are
for cascade outcomes that lint and source scans cannot prove: viewport folds,
theme resolution, primitive states, chart sizing, modal layers, tap-target
floors, and similar behavior. Install the repository-pinned Chromium runtime
once with `npx --no-install playwright install chromium`.

The JSON ledgers have distinct roles:

- `css-literal-debt.json` is exact allowed literal debt;
- `css-retired-classes.json` contains the 74 removed classes and their
  replacements; and
- `css-dynamic-classes.json` contains classes that exist only at runtime.

Do not add an exemption, disable a rule, or weaken a contract to make a change
pass. A guardrail failure is evidence that the visual design or the documented
contract needs a deliberate update.
