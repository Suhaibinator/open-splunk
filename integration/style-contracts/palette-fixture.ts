/**
 * One page's worth of production markup for the palette contracts.
 *
 * The palette probes in `css-contracts.spec.ts` ask questions of every
 * surface at once -- does any element paint its ink in its own ground, does
 * every raised surface keep its alpha, does the mono face reach every control
 * -- so they need the shell, the editor, a table, every button and badge, the
 * modal family, the drawer, the toast, the admin sidebar and the Appearance
 * card on one page. The markup mirrors the DOM the React components render
 * (`app/_components/product-shell.tsx`,
 * `app/search-workspace/components/search-editor.tsx`, the modal component,
 * the shared drawer, `app/admin/appearance-settings.tsx`) closely enough
 * that every class here is one the stylesheets already select on. `SHELL_SELECTORS` names the elements a
 * classic pixel-parity capture compares across a change; the list is long on
 * purpose, because a knob that leaks touches a surface nobody was looking at.
 */

export const SHELL_FIXTURE = `
<div class="suite-shell">
  <a class="skip-link" href="#main">Skip to main content</a>
  <header class="suite-product-bar">
    <button class="drawer-trigger" type="button" aria-label="Open product navigation"><span></span><span></span><span></span></button>
    <a class="wordmark" href="#"><span>open</span><b>splunk</b></a>
    <div class="suite-menu-anchor">
      <button class="suite-app-switcher" type="button" aria-haspopup="menu" aria-expanded="true">App: <strong>Search</strong></button>
      <div class="suite-popover" role="menu">
        <span class="suite-menu-label">Your apps</span>
        <a href="#" role="menuitem"><i class="suite-app-icon">S</i><span><strong>Search</strong><small>Search and reporting</small></span></a>
        <button type="button" role="menuitem"><i class="suite-app-icon suite-app-icon--muted">M</i><span><strong>Monitoring</strong><small>Operations</small></span></button>
      </div>
    </div>
    <div class="suite-utilities">
      <span class="suite-context">demo</span>
      <label class="suite-find"><input type="search" placeholder="Find" /><kbd>⌘K</kbd><button type="button">Go</button></label>
      <span class="health-indicator"><span></span>Healthy</span>
      <a href="#">Activity <span class="activity-count">3</span></a>
      <div class="suite-menu-anchor">
        <button type="button" aria-haspopup="menu" aria-expanded="true">Help</button>
        <div class="floating-menu help-menu" role="menu">
          <span class="menu-label">Help</span>
          <button type="button" role="menuitem"><span>?</span><span><strong>Documentation</strong><small>Reference</small></span></button>
          <button type="button" role="menuitem" class="selected"><span>!</span><span><strong>Shortcuts</strong><small>Keys</small><b>⌘/</b></span></button>
          <div class="menu-separator"></div>
          <div class="user-summary"><span>SU</span><div><strong>suhaib</strong><small>Administrator</small></div></div>
          <span class="suite-menu-rule"></span>
          <fieldset class="suite-theme-menu"><legend class="suite-menu-label">Theme</legend><button role="menuitemradio" type="button" aria-checked="true">System</button></fieldset>
        </div>
      </div>
    </div>
  </header>
  <nav class="suite-app-bar">
    <div class="suite-app-identity"><span>S</span>Search</div>
    <div class="suite-primary-nav"><a href="#" class="active">Search</a><a href="#">Reports</a><a href="#">Alerts</a></div>
  </nav>
  <main class="suite-main" id="main">
    <section class="search-composer">
      <div class="spl-editor focused">
        <div class="editor-gutter"><div class="editor-gutter-lines"><span>1</span><button type="button" class="editor-gutter-marker" data-severity="warning">2</button></div></div>
        <div class="editor-highlight"><span class="spl-command">search</span> <span class="spl-field">host</span><span class="spl-operator">=</span><span class="spl-string">"web"</span> <span class="spl-pipe">|</span> <span class="spl-command">stats</span> <span class="spl-function">count</span> <span class="spl-boolean">AND</span> <span class="spl-error-token">??</span></div>
        <textarea aria-label="Search">search host="web" | stats count</textarea>
      </div>
      <div class="time-picker-wrap">
        <button class="time-range-button" type="button">Last 24 hours</button>
        <dialog class="time-popover" open>
          <header class="time-popover-header"><div><strong>Select time range</strong><small>Presets</small></div><button type="button">×</button></header>
          <div class="time-picker-layout">
            <aside class="time-picker-nav"><button type="button" class="active">Presets</button><button type="button">Relative</button></aside>
            <div class="time-picker-content"><h3>Common</h3><div class="preset-grid"><button type="button" class="selected">Last 15 minutes</button><button type="button">Last hour</button></div></div>
          </div>
        </dialog>
      </div>
      <button class="button run-button" type="button">Search</button>
      <div class="completion-menu" role="listbox">
        <div class="completion-group"><div class="completion-title"><span>Commands</span></div>
          <button type="button" class="completion-option" role="option" aria-selected="true"><code>stats</code><span>Aggregate</span><kbd>Tab</kbd></button>
          <button type="button" class="completion-option" role="option" aria-selected="false"><code>timechart</code><span>Over time</span></button>
        </div>
      </div>
    </section>
    <p class="body-copy">Body copy with <a href="#">a link</a> and <code>code</code>.</p>
    <div class="table-wrap">
      <table class="table">
        <thead><tr><th>Name</th><th class="numeric-data">Count</th></tr></thead>
        <tbody>
          <tr><td><a href="#">Errors by host</a><span class="table-secondary">Saved yesterday</span></td><td class="numeric-data">1,204</td></tr>
          <tr><td><code>index=main</code></td><td class="numeric-data">7</td></tr>
        </tbody>
      </table>
    </div>
    <div class="button-row">
      <button class="button" type="button">Default</button>
      <button class="button button--primary" type="button">Primary</button>
      <button class="button button--secondary" type="button">Secondary</button>
      <button class="button button--danger" type="button">Danger</button>
      <button class="button button--ghost" type="button">Ghost</button>
      <button class="button button--link" type="button">Link</button>
      <button class="button button--icon" type="button" aria-label="Icon">i</button>
      <button class="button button--compact" type="button">Compact</button>
      <button class="button button--toolbar" type="button">Toolbar</button>
      <button class="button" type="button" aria-disabled="true">Disabled</button>
    </div>
    <div class="status-row">
      <span class="status status--dot status--success"></span>
      <span class="status status--dot status--info"></span>
      <span class="status status--dot status--warning"></span>
      <span class="status status--dot status--error"></span>
      <span class="status status--dot status--neutral"></span>
      <span class="status status--dot status--progress"></span>
      <span class="status status--label">Label</span>
      <span class="badge">Badge</span>
      <span class="badge badge--outline">Outline</span>
      <span class="badge badge--success">Success</span>
      <span class="badge badge--info">Info</span>
      <span class="badge badge--warning">Warning</span>
      <span class="badge badge--error">Error</span>
      <span class="badge badge--neutral">Neutral</span>
    </div>
    <form class="form-stack"><label><span>Name</span><input type="text" value="Errors" /><small>Shown in the list</small></label><label><span>Notes</span><textarea>Notes</textarea></label></form>
    <span class="skeleton skeleton--line"></span>
    <div class="admin-layout">
      <aside class="admin-sidebar">
        <span class="admin-sidebar-label">Administration</span>
        <button type="button" class="active"><i>S</i><span><strong>Server</strong><small>Settings and appearance</small></span><b>›</b></button>
        <button type="button"><i>U</i><span><strong>Users</strong><small>Accounts and roles</small></span><b>›</b></button>
      </aside>
      <div class="admin-content">
        <form class="server-settings admin-section-stack">
          <section class="suite-card settings-group">
            <header><h3>Appearance</h3><p>Instance-wide palette shown to every user.</p></header>
            <div class="appearance-palette-options" role="radiogroup" aria-label="Palette">
              <label class="is-selected" for="appearance-palette-classic"><input checked id="appearance-palette-classic" name="appearance-palette" type="radio" value="classic" /><span><strong>Classic</strong><small>The current Splunk-style look.</small></span></label>
              <label for="appearance-palette-ocean"><input id="appearance-palette-ocean" name="appearance-palette" type="radio" value="ocean" /><span><strong>Ocean</strong><small>Cool blue surfaces and slate-blue bars.</small></span></label>
            </div>
          </section>
        </form>
      </div>
    </div>
  </main>
  <div class="modal-layer">
    <div class="modal-backdrop"></div>
    <div class="modal-card" role="dialog">
      <header class="modal-header"><h2>Save search</h2><p>Give it a name.</p></header>
      <div class="modal-body">Body</div>
      <footer class="modal-footer"><button class="button" type="button">Cancel</button><button class="button button--primary" type="button">Save</button></footer>
    </div>
  </div>
  <div class="drawer-backdrop"></div>
  <nav class="drawer" aria-label="Product navigation">
    <header><div><span class="suite-user-avatar">S</span><span><strong>Search</strong><small>Search and reporting</small></span></div><button type="button">×</button></header>
    <span class="drawer-label">Navigate</span>
    <a href="#" class="active"><span>S</span>Search<b>›</b></a>
    <a href="#"><span>R</span>Reports</a>
    <div class="drawer-rule"></div>
    <span class="drawer-app-state">Reading apps</span>
  </nav>
  <div class="toast"><span>i</span><strong>Saved</strong><button type="button">×</button></div>
  <div class="toast toast-success"><span>✓</span><strong>Done</strong></div>
  <div class="toast toast-warning"><span>!</span><strong>Careful</strong></div>
</div>
`;

/** Every element a classic pixel-parity capture compares, one CSS selector each. */
export const SHELL_SELECTORS: readonly string[] = [
  "body",
  ".suite-shell",
  ".skip-link",
  ".suite-product-bar",
  ".drawer-trigger",
  ".wordmark",
  ".wordmark b",
  ".suite-app-switcher",
  ".suite-popover",
  ".suite-menu-label",
  ".suite-popover > a",
  ".suite-popover > button",
  ".suite-app-icon",
  ".suite-utilities",
  ".suite-context",
  ".suite-find",
  ".suite-find input",
  ".suite-find kbd",
  ".health-indicator",
  ".activity-count",
  ".floating-menu",
  ".menu-label",
  ".floating-menu button",
  ".floating-menu button.selected",
  ".floating-menu button strong",
  ".floating-menu button small",
  ".menu-separator",
  ".user-summary",
  ".user-summary small",
  ".suite-theme-menu > button",
  ".suite-app-bar",
  ".suite-app-identity",
  ".suite-primary-nav a",
  ".suite-primary-nav a.active",
  ".search-composer",
  ".spl-editor",
  ".editor-gutter",
  ".editor-gutter-lines span",
  ".editor-gutter-marker",
  ".editor-highlight",
  ".spl-command",
  ".spl-field",
  ".spl-operator",
  ".spl-string",
  ".spl-pipe",
  ".spl-function",
  ".spl-boolean",
  ".spl-error-token",
  ".spl-editor textarea",
  ".time-range-button",
  ".time-popover",
  ".time-popover-header",
  ".time-picker-nav button",
  ".time-picker-nav button.active",
  ".preset-grid button",
  ".preset-grid button.selected",
  ".run-button",
  ".completion-menu",
  ".completion-title",
  ".completion-option",
  '.completion-option[aria-selected="true"]',
  ".completion-menu code",
  ".completion-menu kbd",
  ".body-copy",
  ".body-copy a",
  ".body-copy code",
  ".table-wrap",
  ".table",
  ".table th",
  ".table td",
  ".table a",
  ".table code",
  ".table-secondary",
  ".button",
  ".button--primary",
  ".button--secondary",
  ".button--danger",
  ".button--ghost",
  ".button--link",
  ".button--icon",
  ".button--compact",
  ".button--toolbar",
  '.button[aria-disabled="true"]',
  ".status--success",
  ".status--info",
  ".status--warning",
  ".status--error",
  ".status--neutral",
  ".status--progress",
  ".status--label",
  ".badge",
  ".badge--outline",
  ".badge--success",
  ".badge--info",
  ".badge--warning",
  ".badge--error",
  ".badge--neutral",
  ".form-stack > label > span",
  ".form-stack input",
  ".form-stack textarea",
  ".form-stack label small",
  ".skeleton",
  ".admin-sidebar",
  ".admin-sidebar-label",
  ".admin-sidebar > button",
  ".admin-sidebar > button.active",
  ".admin-sidebar > button.active > i",
  ".admin-sidebar > button small",
  ".admin-sidebar > button.active small",
  ".appearance-palette-options",
  ".appearance-palette-options label",
  ".appearance-palette-options label.is-selected",
  ".appearance-palette-options strong",
  ".appearance-palette-options small",
  ".appearance-palette-options label.is-selected small",
  ".modal-layer",
  ".modal-backdrop",
  ".modal-card",
  ".modal-header h2",
  ".modal-header p",
  ".modal-body",
  ".modal-footer",
  ".drawer-backdrop",
  ".drawer",
  ".drawer > header",
  ".drawer > header small",
  ".drawer-label",
  ".drawer > a",
  ".drawer > a.active",
  ".drawer-rule",
  ".drawer-app-state",
  ".toast",
  ".toast > span",
  ".toast strong",
  ".toast button",
  ".toast-success",
  ".toast-success > span",
  ".toast-warning",
];

/** The computed properties a parity capture reads off each element. */
export const PAINT_PROPERTIES: readonly string[] = [
  "backdropFilter",
  "backgroundColor",
  "backgroundImage",
  "borderBottomColor",
  "borderBottomLeftRadius",
  "borderBottomWidth",
  "borderLeftColor",
  "borderLeftWidth",
  "borderRightColor",
  "borderTopColor",
  "borderTopLeftRadius",
  "borderTopWidth",
  "boxShadow",
  "color",
  "colorScheme",
  "fontFamily",
  "fontSize",
  "fontWeight",
  "letterSpacing",
  "lineHeight",
  "opacity",
  "outlineColor",
  "outlineWidth",
  "textDecorationColor",
  "zIndex",
];

/**
 * The eight knob consumers: the surfaces whose `background` is a
 * `color-mix()` over a translucency knob, which Chromium serialises as
 * `color(srgb …)` rather than `rgb()` even at the inert 100%.
 */
export const KNOB_CONSUMERS: readonly string[] = [
  ".completion-menu",
  ".drawer",
  ".floating-menu",
  ".modal-card",
  ".suite-app-bar",
  ".suite-product-bar",
  ".time-popover",
  ".toast",
];
