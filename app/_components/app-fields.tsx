import type { AppFormState } from "../admin/admin-resource-data";

export function AppFields({ form, onChange, editing }: {
  form: AppFormState;
  onChange: (form: AppFormState) => void;
  editing: boolean;
}) {
  return <div className="admin-form">
    <label htmlFor="app-slug"><span>Slug</span><input id="app-slug" value={form.slug} disabled={editing} required pattern="[a-z0-9][a-z0-9_-]*" onChange={(event) => onChange({ ...form, slug: event.target.value })} /><small>Lowercase durable identifier; it cannot be changed later.</small></label>
    <label htmlFor="app-display-name"><span>Display name</span><input id="app-display-name" value={form.displayName} required onChange={(event) => onChange({ ...form, displayName: event.target.value })} /></label>
    <label htmlFor="app-description"><span>Description <small>(optional)</small></span><input id="app-description" value={form.description} onChange={(event) => onChange({ ...form, description: event.target.value })} /></label>
    <label htmlFor="app-indexes"><span>Default indexes <small>(optional)</small></span><input id="app-indexes" value={form.indexNames} placeholder="main, security" onChange={(event) => onChange({ ...form, indexNames: event.target.value })} /><small>Comma-separated index names used as the app’s default search scope.</small></label>
    <label className="admin-checkbox"><input type="checkbox" aria-label="Configure a default time range" checked={form.hasTimeRange} onChange={(event) => onChange({ ...form, hasTimeRange: event.target.checked })} /><span><strong>Configure a default time range</strong><small>Otherwise consumers use their endpoint defaults.</small></span></label>
    {form.hasTimeRange ? <>
      <label htmlFor="app-earliest"><span>Earliest</span><input id="app-earliest" value={form.earliest} placeholder="-24h" onChange={(event) => onChange({ ...form, earliest: event.target.value })} /></label>
      <label htmlFor="app-latest"><span>Latest</span><input id="app-latest" value={form.latest} placeholder="now" onChange={(event) => onChange({ ...form, latest: event.target.value })} /></label>
      <label htmlFor="app-timezone"><span>Timezone</span><input id="app-timezone" value={form.timezone} placeholder="UTC" onChange={(event) => onChange({ ...form, timezone: event.target.value })} /></label>
    </> : null}
  </div>;
}
