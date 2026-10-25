/**
 * The identity surfaces as the React components render them, filled with the
 * text people actually type: Arabic with trailing punctuation, a 225-character
 * word, CJK, an emoji with a skin tone, a flag, escaped markup, and an address
 * at the length the mail RFCs allow. The markup mirrors
 * `app/_components/product-shell.tsx` and `product-identity.tsx` (the bar, the
 * organization switcher and the user menu), `app/admin/members-panel.tsx`
 * (context line, a card-mode table, an error state inside a members card),
 * `app/admin/site-*-panel.tsx` (the capped description) and
 * `app/signin/sign-in-completion.tsx` / `auth-frame.tsx` (the identity
 * summary and both notice tones), closely enough that every class here is one
 * the stylesheets already select on.
 *
 * Kept in step with `lib/testing/hostile-text.ts`, which the mount tests use;
 * the strings are spelled out here because this file is served as markup.
 */

const LONG_WORD = "Internationalisierungsverwaltungsgesellschaft".repeat(5);
const LONG_EMAIL = `${"firstname.lastname.".repeat(3)}x@${"subdomain.".repeat(12)}example.test`;
const RTL = "شركة الأمن السيبراني ٢٠٢٦!";
const DISPLAY_NAME = "👩🏽‍💻Zoë 🇳🇿Ngata";
const DESCRIPTION = `Regional SOC for ${RTL} and partners — `.repeat(6).trim();

/** The product bar with the switcher and user menu open, so their rows are measured too. */
export const IDENTITY_BAR_FIXTURE = `
<div class="suite-shell">
  <header class="suite-product-bar">
    <button class="drawer-trigger" type="button" aria-label="Open product navigation"><span></span><span></span><span></span></button>
    <a class="wordmark" href="#"><span>open</span><b>splunk</b></a>
    <div class="suite-menu-anchor suite-org-anchor">
      <button class="suite-org-switcher" type="button" aria-haspopup="menu" aria-expanded="false">
        <span class="suite-org-switcher-label">Org:</span>
        <strong title="${LONG_WORD}"><bdi>${LONG_WORD}</bdi></strong>
      </button>
    </div>
    <nav class="suite-utilities" aria-label="Product utilities">
      <span class="suite-context">Demo workspace</span>
      <div class="suite-menu-anchor">
        <button class="suite-user-button" type="button" aria-label="${DISPLAY_NAME} menu" aria-haspopup="menu" aria-expanded="false">
          <span>👩🏽‍💻🇳🇿</span><b title="${DISPLAY_NAME} ${LONG_WORD}"><bdi>${DISPLAY_NAME} ${LONG_WORD}</bdi></b>
        </button>
      </div>
    </nav>
  </header>
</div>`;

/** The two menus, rendered in place so their rows are read on the ground they paint. */
export const IDENTITY_MENUS_FIXTURE = `
<div class="suite-shell">
  <header class="suite-product-bar">
    <div class="suite-menu-anchor suite-org-anchor">
      <button class="suite-org-switcher" type="button" aria-haspopup="menu" aria-expanded="true">
        <span class="suite-org-switcher-label">Org:</span>
        <strong title="${RTL}"><bdi>${RTL}</bdi></strong>
      </button>
      <div class="suite-popover suite-org-popover" role="menu" aria-label="Organizations" data-suite-menu="org">
        <span class="suite-menu-label">Organizations</span>
        <button class="identity-probe-selected" role="menuitemradio" type="button" aria-checked="true">
          <i class="suite-app-icon suite-org-icon" aria-hidden="true">ش</i>
          <span><strong><bdi>${RTL}</bdi></strong><small>Owner</small></span>
        </button>
        <button class="identity-probe-row" role="menuitemradio" type="button" aria-checked="false">
          <i class="suite-app-icon suite-org-icon" aria-hidden="true">I</i>
          <span><strong><bdi>${LONG_WORD}</bdi></strong><small>Member · inherited</small></span>
        </button>
        <button role="menuitemradio" type="button" aria-checked="false">
          <i class="suite-app-icon suite-org-icon" aria-hidden="true">&lt;</i>
          <span><strong><bdi>&lt;img src=x onerror=alert(1)&gt; &amp; Söhne</bdi></strong><small>Viewer</small></span>
        </button>
      </div>
    </div>
    <nav class="suite-utilities" aria-label="Product utilities">
      <div class="suite-menu-anchor">
        <button class="suite-user-button" type="button" aria-haspopup="menu" aria-expanded="true">
          <span>👩🏽‍💻🇳🇿</span><b title="${DISPLAY_NAME}"><bdi>${DISPLAY_NAME}</bdi></b>
        </button>
        <div class="suite-popover suite-utility-popover suite-user-popover" role="menu" data-suite-menu="user">
          <div class="suite-user-summary">
            <span aria-hidden="true">👩🏽‍💻🇳🇿</span>
            <div>
              <strong><bdi>${DISPLAY_NAME}</bdi></strong>
              <small><bdi>${LONG_EMAIL}</bdi></small>
              <small><bdi>${RTL}</bdi></small>
            </div>
          </div>
          <a role="menuitem" href="#">Administration</a>
        </div>
      </div>
    </nav>
  </header>
</div>`;

/** The members panel and a site-directory row, the content an administrator scans. */
export const IDENTITY_PANEL_FIXTURE = `
<section class="admin-section-stack" data-members-state="ready">
  <p class="members-context"><span><strong><bdi>${LONG_WORD}</bdi></strong> You manage members as an owner.</span></p>
  <section class="suite-card resource-table-card members-card">
    <header class="suite-card-header"><div><h3>Members</h3><p>Explicit roles are granted here.</p></div><span>2 members</span></header>
    <div class="table-wrap">
      <table class="table table--cards members-table">
        <caption class="sr-only">Members of <bdi>${LONG_WORD}</bdi></caption>
        <thead><tr><th scope="col">Member</th><th scope="col">Explicit role</th><th scope="col">Effective role</th><th scope="col">Status</th><th scope="col">Last active</th></tr></thead>
        <tbody>
          <tr data-member-id="1">
            <td data-label="Member"><span class="members-person"><strong><bdi>${DISPLAY_NAME}</bdi></strong><small><bdi>${LONG_EMAIL}</bdi></small></span></td>
            <td data-label="Explicit role"><span class="badge">Owner</span></td>
            <td data-label="Effective role"><span class="badge">Owner</span><small class="members-note">Inherited from <bdi>${RTL}</bdi></small></td>
            <td data-label="Status">Active</td>
            <td data-label="Last active">Never</td>
          </tr>
          <tr data-member-id="2">
            <td data-label="Member"><span class="members-person"><strong><bdi>${LONG_WORD}</bdi></strong><small><bdi>${"q".repeat(64)}@example.test</bdi></small></span></td>
            <td data-label="Explicit role"><span class="members-muted">None</span></td>
            <td data-label="Effective role"><span class="badge">Viewer</span></td>
            <td data-label="Status">Active</td>
            <td data-label="Last active">Never</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
  <section class="suite-card resource-table-card members-card identity-probe-error-card">
    <header class="suite-card-header"><div><h3>Pending invitations</h3></div></header>
    <div class="backend-resource-state backend-resource-state--error" role="alert">
      <span>!</span>
      <div><strong>Invitations could not be loaded</strong><p>${LONG_WORD}</p></div>
      <button type="button">Retry</button>
    </div>
  </section>
  <div class="table-wrap">
    <table class="table table--cards site-directory-organizations-table">
      <thead><tr><th scope="col">Organization</th><th scope="col">Description</th></tr></thead>
      <tbody>
        <tr data-organization-id="9">
          <td data-label="Organization"><strong><bdi>${RTL}</bdi></strong></td>
          <td data-label="Description"><span class="site-directory-description" title="${DESCRIPTION}"><bdi>${DESCRIPTION}</bdi></span></td>
        </tr>
        <tr data-organization-id="10">
          <td data-label="Organization"><strong><bdi>${LONG_WORD}</bdi></strong></td>
          <td data-label="Description"><span class="site-directory-muted">No description</span></td>
        </tr>
      </tbody>
    </table>
  </div>
</section>
<div class="signin-card">
  <div class="signin-identity">
    <span class="signin-identity-mark" aria-hidden="true">👩🏽‍💻🇳🇿</span>
    <span><b><bdi>${DISPLAY_NAME}</bdi></b><small><bdi>${LONG_EMAIL}</bdi></small><small><bdi>${RTL}</bdi></small></span>
  </div>
  <div class="signin-help-notice signin-message" role="status" data-tone="info"><span aria-hidden="true">i</span><div><strong>Check your inbox</strong><p>We sent a link to <a href="#">${LONG_EMAIL}</a>.</p></div></div>
  <div class="signin-help-notice signin-message" role="status" data-tone="success"><span aria-hidden="true">✓</span><div><strong>Email verified</strong><p>Continue to <a href="#">${RTL}</a>.</p></div></div>
</div>`;

/** Elements whose ink a reader depends on, held to the palette's text floor. */
export const IDENTITY_READABLE_TEXT: readonly string[] = [
  ".suite-org-switcher > strong",
  ".suite-user-button > b",
  ".identity-probe-selected strong",
  ".identity-probe-selected small",
  ".identity-probe-row strong",
  ".identity-probe-row small",
  ".suite-user-summary strong",
  ".suite-user-summary small",
  ".members-context strong",
  ".members-person strong",
  ".members-person small",
  ".members-note",
  ".members-muted",
  ".backend-resource-state strong",
  ".backend-resource-state p",
  ".site-directory-description",
  ".site-directory-muted",
  ".signin-identity b",
  ".signin-identity small",
  '.signin-help-notice[data-tone="info"] p',
  '.signin-help-notice[data-tone="info"] a',
  '.signin-help-notice[data-tone="success"] p',
  '.signin-help-notice[data-tone="success"] a',
];

/** Avatars and marks: their initials read against their own fill. */
export const IDENTITY_MARKS: readonly string[] = [
  ".suite-user-button > span",
  ".identity-probe-selected .suite-org-icon",
  ".identity-probe-row .suite-org-icon",
  ".suite-user-summary > span",
  ".signin-identity-mark",
];
