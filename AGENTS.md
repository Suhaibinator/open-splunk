# Agent guidelines

Go backend (`cmd/`, `internal/`) + Next.js UI (`app/`, `lib/`), UI embedded into the server binary. Read this before changing anything; `docs/theming.md` is the full styling reference.

## Styling — the rules that keep the UI consistent

1. **No literals outside the token files.** Every colour, font, size, radius, shadow and z-index reads a token from `app/styles/tokens-color.css` / `tokens-scale.css`. No `#hex`, `rgb()`, named colours, `system-ui`, `13px`, `z-index: 999` in any other stylesheet, inline `style`, or TS constant. If no semantic token fits, add one (tier 2, with a one-line role comment) — never reach into the tier-1 palette from a component.
2. **One styling lane.** Plain CSS, feature-prefixed kebab-case (or BEM) class names. No CSS modules, no CSS-in-JS, no `:global()`, no inline colour. State is expressed with `data-*`/`aria-*` attributes and `is-*` classes, not inline style toggles.
3. **One primitive per concept.** `.button`, `.table`, `.status`, `.badge`, the modal family, `ProductShell`, `Wordmark`, the drawer. Extend with a modifier; never fork a second implementation or restyle a primitive from a feature file.
4. **Where a rule lives.** Shared: `app/styles/base.css` and `app/styles/primitives/*.css`. Feature: the `*.css` beside the feature. Every stylesheet is imported exactly once, in cascade order, from `app/styles/index.css`. Responsive rules sit beside their base rules; breakpoints are only `1240 / 980 / 760 / 480px` (max-width).
5. **Verify deliberate visual changes** with the structural invariants, stylesheet lint, and computed-style contracts; describe intentional appearance changes in the commit body.

## Verification — run before claiming done

```
npm run typecheck && npm run lint && npm run check:docs
npm run test:frontend      # unit + style invariants
npm run test:contracts     # computed-style contracts (Playwright)
go build ./... && go vet ./... && go test ./...
```

- `npm run lint` includes `lint:css` (stylelint, errors). Go lint in CI is `golangci-lint` pinned in `.github/workflows/ci.yml` — run it with `GOOS=linux` and uncapped issue counts.
- Install the pinned browser once with `npx --no-install playwright install chromium` before `npm run test:contracts`.
- `scripts/test-frontend.mjs` has a **hardcoded** test list; a new `*.test.ts(x)` or `scripts/*.test.mjs` runs only if added there.
- `scripts/check-docs.mjs` allow-lists `docs/*.md`; register a new doc in both lists or `check:docs` fails.
- `internal/clickhouse/testdata/golden/*.sql` snapshots the compiled contract of every official SPL corpus case. A lowering change shows up as a diff there; regenerate with `OPEN_SPLUNK_UPDATE_GOLDEN=1 go test ./internal/clickhouse -run TestOfficial` (refused under `CI`) and review the diff before committing.
- Integration suites are gated by env: `OPEN_SPLUNK_DEVELOPMENT_INTEGRATION=1`, `OPEN_SPLUNK_BACKEND_INTEGRATION=1`, `OPEN_SPLUNK_CLICKHOUSE_INTEGRATION=1`, `OPEN_SPLUNK_OCI_INTEGRATION=1`, `OPEN_SPLUNK_BACKEND_LOAD=1`, `OPEN_SPLUNK_HEC_LOAD=1`, `OPEN_SPLUNK_HEC_SOAK=1`, and `OPEN_SPLUNK_HEC_SLOW_CLIENT=1` (need Docker). Run the one your change touches; see `integration/README.md` and `docs/hec.md` for exact commands.
- Verification means executed commands with output, not code reading.

## Code and repo hygiene

- Fix lint findings; never add `eslint-disable`/`stylelint-disable`/`nolint` or grandfather lists. Never weaken or delete a test to make it pass.
- Match surrounding style: 2-space indent, alphabetised declarations inside CSS blocks, one selector per line.
- No new runtime dependencies without a stated reason.
- Stage files by explicit path (`git add <file>`), never `git add -A`. No `git stash`, `reset --hard`, force-push, or rebasing a branch other agents are integrating into. Do not push unless asked.
- Commit subjects: imperative, `type(scope): …`, body says *why* and lists any deliberate visual change.
