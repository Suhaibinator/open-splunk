"use client";

import { useEffect, useMemo, useState, type FormEvent } from "react";
import type { OrganizationMemberDetails } from "@suhaibinator/go-common-core/lib/generated/organization";
import { summarizeByteQuantity } from "@/lib/byte-quantity";
import type { RetainedStorageUsage } from "@/gen/ts/open_splunk/system_api";
import type { IndexCapabilityDecision, IndexPermission } from "@/gen/ts/open_splunk/index_permissions_api";
import { createIdentityClient, refreshIdentity } from "@/lib/api/identity-client";
import { isSiteAdministrator, selectOrganization } from "@/lib/api/identity-session";
import { ORGANIZATION_ROLES as ROLES, canManageOrganizationRole, grantableOrganizationRoles } from "@/lib/api/organization-role-policy";
import { isHttpStatus } from "@/lib/api/protobuf-transport";
import { createOpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import { useIdentity } from "../_components/identity-provider";
import { Select, SelectOption } from "../_components/select";
import { PageHeading } from "../_components/product-shell";

function positiveId(value: string): bigint {
  if (!/^[1-9]\d*$/u.test(value)) throw new Error("Enter a positive account ID.");
  const id = BigInt(value);
  if (id > (1n << 63n) - 1n) throw new Error("The account or organization ID is out of range.");
  return id;
}

export function OrganizationAccess({ apiBaseUrl }: { apiBaseUrl: string }) {
  const identity = useIdentity();
  const client = useMemo(() => createIdentityClient(apiBaseUrl), [apiBaseUrl]);
  const api = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const organization = identity.organizations.find((org) => org.id === identity.organizationId);
  const siteAdministrator = isSiteAdministrator();
  const canManage = siteAdministrator || organization?.role === 23 || organization?.role === 44;
  const grantableRoles = grantableOrganizationRoles(organization?.role ?? 0, siteAdministrator);
  const [storageUsage, setStorageUsage] = useState<RetainedStorageUsage | null>(null);
  const [members, setMembers] = useState<OrganizationMemberDetails[]>([]);
  const [page, setPage] = useState(1);
  const [hasNext, setHasNext] = useState(false);
  const [userId, setUserId] = useState("");
  const [role, setRole] = useState(1);
  const selectedRole = grantableRoles.some((item) => item.value === role) ? role : 1;
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [organizationName, setOrganizationName] = useState("");
  const [organizationSlug, setOrganizationSlug] = useState("");
  const [ownerId, setOwnerId] = useState("");
  const [parentId, setParentId] = useState("");
  const [indexId, setIndexId] = useState("");
  const [indexOrganization, setIndexOrganization] = useState<bigint | null>(null);
  const [destinationOrganization, setDestinationOrganization] = useState(identity.organizationId?.toString() ?? "");
  const [rules, setRules] = useState<IndexPermission[]>([]);
  const [indexRevision, setIndexRevision] = useState(0n);
  const [effective, setEffective] = useState<IndexCapabilityDecision[]>([]);
  const [subjectKind, setSubjectKind] = useState("role");
  const [subjectId, setSubjectId] = useState("1");
  const [capability, setCapability] = useState("search");
  const [effect, setEffect] = useState("allow");

  useEffect(() => {
    if (identity.organizationId === null) return;
    let current = true;
    void client.organizations.members.list(identity.organizationId, { page }).then((response) => {
      if (!current) return;
      setMembers(response.members); setHasNext(response.pagination?.hasNextPage ?? false);
    }).catch((reason: unknown) => { if (current) setError(reason instanceof Error ? reason.message : "Unable to list members."); });
    return () => { current = false; };
  }, [client, identity.organizationId, page]);

  useEffect(() => {
    if (identity.organizationId === null) return;
    const controller = new AbortController();
    void api.system.bootstrap({}, { signal: controller.signal }).then((response) => {
      const usage = response.retainedStorageUsage;
      if (!controller.signal.aborted && usage !== undefined && usage.organizationId === identity.organizationId?.toString()) setStorageUsage(usage);
    }).catch((reason: unknown) => { if (!controller.signal.aborted) setError(reason instanceof Error ? reason.message : "Unable to read storage usage."); });
    return () => controller.abort();
  }, [api, identity.organizationId]);

  async function action(work: () => Promise<void>) {
    if (busy) return;
    setBusy(true); setError(null); setNotice(null);
    try { await work(); } catch (reason) { if (isHttpStatus(reason, 409) && indexOrganization !== null) {
        await loadRules();
        setError("Permissions changed in another session. Review the refreshed rules and retry your change.");
      } else setError(reason instanceof Error ? reason.message : "The operation failed."); } finally { setBusy(false); }
  }
  async function reloadMembers() {
    if (identity.organizationId === null) return;
    const response = await client.organizations.members.list(identity.organizationId, { page });
    setMembers(response.members); setHasNext(response.pagination?.hasNextPage ?? false);
  }
  function addMember(event: FormEvent) {
    event.preventDefault();
    void action(async () => {
      if (identity.organizationId === null) throw new Error("Select an organization first.");
      await client.organizations.members.add(identity.organizationId, positiveId(userId), selectedRole);
      await reloadMembers(); setUserId(""); setNotice("Member added.");
    });
  }
  function createOrganization(event: FormEvent) {
    event.preventDefault();
    void action(async () => {
      const owner = positiveId(ownerId);
      const parent = isSiteAdministrator() ? (parentId ? positiveId(parentId) : null) : identity.organizationId;
      if (owner > BigInt(Number.MAX_SAFE_INTEGER) || (parent !== null && parent > BigInt(Number.MAX_SAFE_INTEGER))) throw new Error("The account or parent ID exceeds this form’s supported range.");
      const response = await fetch(`${apiBaseUrl}/api/identity/organizations/create-with-owner`, { method: "POST", credentials: "include", cache: "no-store", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ name: organizationName, slug: organizationSlug, owner_user_id: Number(owner), ...(parent === null ? {} : { parent_organization_id: Number(parent) }) }) });
      if (!response.ok) throw new Error("Unable to create the organization. Check the name, owner account and parent permissions.");
      await refreshIdentity(client); setOrganizationName(""); setOrganizationSlug(""); setOwnerId(""); setNotice("Organization created with the selected owner.");
    });
  }
  async function loadRules() {
    const response = await api.indexPermissions.get({ indexId: indexId.trim() });
    setRules(response.rules); setIndexOrganization(response.organizationId); setIndexRevision(response.revision); setEffective(response.effective);
  }
  return <>
    <PageHeading title="Organizations and index access" description="Membership comes first. Index rules grant or deny operations inside an organization; site administrators have access to all indexes." />
    {error ? <p role="alert">{error}</p> : null}{notice ? <p role="status">{notice}</p> : null}
    <section className="panel"><h2>Your account</h2><p>{identity.user?.email} · Account ID {identity.user?.id.toString()}</p><p>{isSiteAdministrator() ? "Site administrator" : organization ? ROLES.find((item) => item.value === organization.role)?.label ?? "Member" : "No organization membership"}</p>
      {identity.organizations.map((org) => <button key={org.id.toString()} className="button button--secondary" type="button" disabled={busy} aria-pressed={org.id === identity.organizationId} onClick={() => selectOrganization(org.id)}>{org.name}</button>)}
    </section>
    {storageUsage ? <section className="panel"><h2>Retained storage</h2><p>{organization?.name ?? "Selected organization"}: {summarizeByteQuantity(storageUsage.organizationUsedBytes)} used of {summarizeByteQuantity(storageUsage.organizationLimitBytes)}</p><p>Instance: {summarizeByteQuantity(storageUsage.globalUsedBytes)} used of {summarizeByteQuantity(storageUsage.globalLimitBytes)}</p><p>Usage includes reserved captures and exports. This snapshot refreshes when you switch organizations or select Refresh.</p><button className="button button--secondary" type="button" disabled={busy} onClick={() => { void action(async () => { const response = await api.system.bootstrap({}); const usage = response.retainedStorageUsage; if (usage === undefined || usage.organizationId !== identity.organizationId?.toString()) throw new Error("Storage usage is unavailable for the selected organization."); setStorageUsage(usage); }); }}>Refresh storage usage</button></section> : null}
    {organization ? <section className="panel"><h2>{organization.name} members</h2><div className="table-wrap"><table className="table"><thead><tr><th>Account</th><th>Role</th>{canManage ? <th>Actions</th> : null}</tr></thead><tbody>{members.map((member) => <tr key={member.userId.toString()}><td>{member.user?.displayPrimary || member.userId.toString()}</td><td>{ROLES.find((item) => item.value === member.effectiveRole)?.label ?? "Member"}</td>{canManage ? <td>{canManageOrganizationRole(organization.role, member.effectiveRole, siteAdministrator) ? <label>Role <Select aria-label={`Role for account ${member.userId}`} value={String(member.explicitRole)} disabled={busy} onValueChange={(value) => { const nextRole = Number(value); void action(async () => { await client.organizations.members.updateRole(organization.id, member.userId, nextRole); await reloadMembers(); }); }}>{grantableRoles.map((item) => <SelectOption key={item.value} value={String(item.value)}>{item.label}</SelectOption>)}</Select></label> : null}{canManageOrganizationRole(organization.role, member.effectiveRole, siteAdministrator) || member.userId === identity.user?.id ? <button className="button button--secondary" type="button" disabled={busy} onClick={() => { void action(async () => { await client.organizations.members.remove(organization.id, member.userId); await reloadMembers(); }); }}>Remove</button> : null}</td> : null}</tr>)}</tbody></table></div><button className="button button--secondary" disabled={busy || page <= 1} onClick={() => setPage((value) => value - 1)}>Previous</button><span>Page {page}</span><button className="button button--secondary" disabled={busy || !hasNext} onClick={() => setPage((value) => value + 1)}>Next</button>
      {canManage ? <form className="form-stack" onSubmit={addMember}><h3>Add an existing account</h3><label>Account ID <input required inputMode="numeric" value={userId} onChange={(event) => setUserId(event.target.value)} /></label><label>Role <Select value={String(selectedRole)} onValueChange={(value) => setRole(Number(value))}>{grantableRoles.map((item) => <SelectOption key={item.value} value={String(item.value)}>{item.label}</SelectOption>)}</Select></label><button className="button" disabled={busy} type="submit">Add member</button></form> : null}
    </section> : null}
    {canManage ? <section className="panel"><form className="form-stack" onSubmit={createOrganization}><h2>{isSiteAdministrator() ? "Create organization" : "Create child organization"}</h2><label>Name <input required value={organizationName} onChange={(event) => setOrganizationName(event.target.value)} /></label><label>Slug <input required value={organizationSlug} onChange={(event) => setOrganizationSlug(event.target.value)} /></label><label>Initial owner account ID <input required inputMode="numeric" value={ownerId} onChange={(event) => setOwnerId(event.target.value)} /></label>{isSiteAdministrator() ? <label>Parent organization ID (optional) <input inputMode="numeric" value={parentId} onChange={(event) => setParentId(event.target.value)} /></label> : <p>Parent: {organization?.name}</p>}<p>The owner must already have an account. Child owners must belong to their parent organization.</p><button className="button" disabled={busy} type="submit">Create organization</button></form></section> : null}
    {identity.organizationId !== null || isSiteAdministrator() ? <section className="panel"><h2>Index permissions</h2><p>Allow grants an operation; Deny overrides allows. A user rule never creates organization membership.</p><form className="form-stack" onSubmit={(event) => { event.preventDefault(); void action(loadRules); }}><label>Index ID <input required value={indexId} onChange={(event) => { setIndexId(event.target.value); setRules([]); setIndexOrganization(null); setEffective([]); setIndexRevision(0n); }} /></label><button className="button button--secondary" disabled={busy} type="submit">Load permissions</button></form>
      {indexOrganization !== null ? <><p>Organization ID {indexOrganization.toString()} · Permissions revision {indexRevision.toString()}</p><table className="table"><thead><tr><th>Operation</th><th>Your access</th><th>Reason</th></tr></thead><tbody>{effective.map((decision) => <tr key={decision.capability}><td>{decision.capability.replaceAll("_", " ")}</td><td>{decision.allowed ? "Allowed" : "Denied"}</td><td>{decision.reason.replaceAll("_", " ")}</td></tr>)}</tbody></table><table className="table"><thead><tr><th>Subject</th><th>Operation</th><th>Effect</th><th>Actions</th></tr></thead><tbody>{rules.map((rule) => <tr key={`${rule.subjectKind}:${rule.subjectId}:${rule.capability}`}><td>{rule.subjectKind === "role" ? ROLES.find((item) => BigInt(item.value) === rule.subjectId)?.label ?? rule.subjectId.toString() : `Account ${rule.subjectId}`}</td><td>{rule.capability.replaceAll("_", " ")}</td><td>{rule.effect}</td><td><button className="button button--secondary" disabled={busy} onClick={() => { void action(async () => { await api.indexPermissions.delete({ indexId: indexId.trim(), permission: rule, expectedRevision: indexRevision }); await loadRules(); }); }}>Remove rule</button></td></tr>)}</tbody></table>
        <form className="form-stack" onSubmit={(event) => { event.preventDefault(); void action(async () => { await api.indexPermissions.set({ indexId: indexId.trim(), permission: { subjectKind, subjectId: positiveId(subjectId), capability, effect }, expectedRevision: indexRevision }); await loadRules(); }); }}><h3>Set a rule</h3><label htmlFor="index-rule-subject">Subject <Select id="index-rule-subject" value={subjectKind} onValueChange={(value) => { setSubjectKind(value); setSubjectId("1"); }}><SelectOption value="role">Organization role</SelectOption><SelectOption value="user">Organization member</SelectOption></Select></label>{subjectKind === "role" ? <label>Role <Select value={subjectId} onValueChange={(value) => setSubjectId(value)}>{ROLES.map((item) => <SelectOption key={item.value} value={String(item.value)}>{item.label}</SelectOption>)}</Select></label> : <label>Member account ID <input required inputMode="numeric" value={subjectId} onChange={(event) => setSubjectId(event.target.value)} /></label>}<label>Operation <Select value={capability} onValueChange={(value) => setCapability(value)}>{["search", "ingest", "manage", "manage_grants"].map((item) => <SelectOption key={item} value={item}>{item.replaceAll("_", " ")}</SelectOption>)}</Select></label><label htmlFor="index-rule-effect">Effect <Select id="index-rule-effect" value={effect} onValueChange={(value) => setEffect(value)}><SelectOption value="allow">Allow</SelectOption><SelectOption value="deny">Deny</SelectOption></Select></label><button className="button" disabled={busy} type="submit">Save rule</button></form>
        {isSiteAdministrator() || (organization?.role ?? 0) >= 44 ? <form className="form-stack" onSubmit={(event) => { event.preventDefault(); void action(async () => { await api.indexPermissions.setOrganization({ indexId: indexId.trim(), organizationId: positiveId(destinationOrganization), expectedRevision: indexRevision }); await loadRules(); }); }}><label>Destination organization <Select value={destinationOrganization} onValueChange={(value) => setDestinationOrganization(value)}>{identity.organizations.map((item) => <SelectOption key={item.id.toString()} value={item.id.toString()}>{item.name}</SelectOption>)}</Select></label><p>Moving an index requires ownership of its current and destination organization.</p><button className="button button--secondary" disabled={busy || !destinationOrganization} type="submit">Assign index organization</button></form> : null}
      </> : null}
    </section> : null}
  </>;
}
