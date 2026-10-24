"use client";

import { useCallback, useEffect, useId, useMemo, useRef, useState, type FormEvent } from "react";

import type { IdentityClient, IdentityError, IdentityOrganization, Page } from "@/lib/api/identity";

import { BackendResourceState } from "../_components/backend-resource-state";
import { formatMediumDateTime } from "../_components/date-format";
import { FieldNote, fieldControlProps } from "../_components/field-validation";
import { useIdentity } from "../_components/identity-provider";
import { Modal } from "../_components/modal";
import { Select, SelectOption } from "../_components/select";
import {
  EMPTY_CREATE_ORGANIZATION_DRAFT,
  ORGANIZATION_KIND_LABELS,
  ORGANIZATION_PICKER_LIMIT,
  SITE_DIRECTORY_PAGE_SIZE,
  asIdentityError,
  createOrganizationErrorField,
  createOrganizationInput,
  hasErrors,
  organizationAncestry,
  organizationKind,
  organizationNames,
  presentMutationError,
  validateCreateOrganizationDraft,
  withOrganizationName,
  type CreateOrganizationDraft,
  type CreateOrganizationErrors,
  type OrganizationKind,
} from "./site-directory-data";
import {
  DirectoryErrorState,
  DirectoryFormError,
  DirectoryHeader,
  DirectoryPager,
  DirectorySearchForm,
  DirectorySkeletonRows,
  DirectoryToast,
  SiteDirectoryAccessState,
  focusAfterCommit,
  useDirectoryPage,
  useEmptyPageFallback,
  useSiteDirectoryAccess,
} from "./site-directory-shared";

const ORGANIZATIONS_TITLE = "Organizations";
const ORGANIZATIONS_DESCRIPTION = "Every tenant on this server and where it sits in the hierarchy. A child organization inherits roles from its ancestors.";
const ORGANIZATION_TABLE_COLUMNS = 5;

const KIND_BADGE: Readonly<Record<OrganizationKind, string>> = {
  child: "badge badge--neutral",
  personal: "badge badge--warning",
  top_level: "badge badge--info",
};

export interface SiteOrganizationsPanelProps {
  readonly pageSize?: number;
}

/** The site-admin Organizations panel: gated on `orgs.admin`, then the directory. */
export function SiteOrganizationsPanel({ pageSize = SITE_DIRECTORY_PAGE_SIZE }: SiteOrganizationsPanelProps) {
  const access = useSiteDirectoryAccess("organizations");
  const headingId = useId();
  if (!access.allowed) {
    return (
      <section className="admin-section-stack" aria-labelledby={headingId}>
        <DirectoryHeader
          headingId={headingId}
          title={ORGANIZATIONS_TITLE}
          description={ORGANIZATIONS_DESCRIPTION}
          actionLabel="Create organization"
          onAction={() => undefined}
          disabledReason={access.reason}
        />
        <SiteDirectoryAccessState subject="organizations" />
      </section>
    );
  }
  return <OrganizationsDirectory client={access.client} headingId={headingId} pageSize={pageSize} />;
}

/** One wide page of organizations: the parent picker's options and the names the hierarchy column reads. */
function usePickerDirectory(client: IdentityClient, generation: number): { readonly page: Page<IdentityOrganization> | null; readonly failed: boolean } {
  const [settled, setSettled] = useState<{ generation: number; page: Page<IdentityOrganization> | null; failed: boolean } | null>(null);
  useEffect(() => {
    let live = true;
    client.adminListOrganizations({}, { limit: ORGANIZATION_PICKER_LIMIT, page: 1 }).then(
      (page) => {
        if (live) setSettled({ failed: false, generation, page });
      },
      () => {
        if (live) setSettled((previous) => ({ failed: true, generation, page: previous?.page ?? null }));
      },
    );
    return () => {
      live = false;
    };
  }, [client, generation]);
  return { failed: settled?.generation === generation && settled.failed, page: settled?.page ?? null };
}

function OrganizationsDirectory({ client, headingId, pageSize }: {
  readonly client: IdentityClient;
  readonly headingId: string;
  readonly pageSize: number;
}) {
  const identity = useIdentity();
  const [searchDraft, setSearchDraft] = useState("");
  const [search, setSearch] = useState("");
  const [pageNumber, setPageNumber] = useState(1);
  const [creating, setCreating] = useState(false);
  const [toast, setToast] = useState<string | null>(null);
  const [pickerGeneration, setPickerGeneration] = useState(0);
  const searchRef = useRef<HTMLInputElement>(null);

  const key = JSON.stringify([search, pageNumber, pageSize]);
  const load = useCallback(
    () => client.adminListOrganizations(search.length === 0 ? {} : { search }, { limit: pageSize, page: pageNumber }),
    [client, pageNumber, pageSize, search],
  );
  const directory = useDirectoryPage(key, load);
  const picker = usePickerDirectory(client, pickerGeneration);
  const errorKind = directory.error?.kind ?? null;

  const { refresh } = identity;
  useEffect(() => {
    if (errorKind === "unauthenticated" || errorKind === "forbidden") refresh().catch(() => undefined);
  }, [errorKind, refresh]);

  const page = directory.page;
  const names = useMemo(
    () => organizationNames(picker.page?.items ?? [], page?.items ?? []),
    [page, picker.page],
  );
  const steppingBack = useEmptyPageFallback(directory, setPageNumber);
  const busy = directory.status === "loading" || steppingBack;

  function applySearch() {
    setSearch(searchDraft.trim());
    setPageNumber(1);
  }

  function clearSearch() {
    setSearchDraft("");
    setSearch("");
    setPageNumber(1);
    // Already clear (an invalid-search error with nothing typed): ask again.
    directory.reload();
    focusAfterCommit(searchRef.current);
  }

  let body;
  if (directory.status === "error" && directory.error !== null) {
    body = (
      <DirectoryErrorState
        error={directory.error}
        errorAt={directory.errorAt}
        subject="organizations"
        onRetry={directory.reload}
        onClearSearch={clearSearch}
      />
    );
  } else if (page === null) {
    body = (
      <div className="suite-card resource-table-card">
        <div className="table-wrap">
          <table className="table site-directory-organizations-table" aria-busy="true">
            <caption className="sr-only">Loading organizations…</caption>
            <OrganizationsTableHead />
            <tbody><DirectorySkeletonRows columns={ORGANIZATION_TABLE_COLUMNS} /></tbody>
          </table>
        </div>
        <p className="sr-only" role="status">Loading organizations…</p>
      </div>
    );
  } else if (page.items.length === 0 && !busy) {
    body = search.length > 0 ? (
      <BackendResourceState
        kind="empty"
        title="No organizations match"
        message={`Nothing matches “${search}”. Search by organization name or slug.`}
        action={<button type="button" onClick={clearSearch}>Clear search</button>}
      />
    ) : (
      <BackendResourceState
        kind="empty"
        title="No organizations yet"
        message="Create the first organization to give its members a tenant of their own."
        action={<button type="button" onClick={() => setCreating(true)}>Create organization</button>}
      />
    );
  } else {
    body = (
      <div className="suite-card resource-table-card">
        <div className="table-wrap">
          <table className="table table--cards site-directory-organizations-table" aria-busy={busy}>
            <caption className="sr-only">Organizations{search.length > 0 ? ` matching ${search}` : ""}</caption>
            <OrganizationsTableHead />
            <tbody>
              {page.items.map((organization) => (
                <OrganizationRow key={organization.id} organization={organization} names={names} />
              ))}
            </tbody>
          </table>
        </div>
        <DirectoryPager label="Organizations pages" noun="organizations" page={page} busy={busy} onPage={setPageNumber} />
      </div>
    );
  }

  return (
    <section className="admin-section-stack" aria-labelledby={headingId}>
      <DirectoryHeader
        headingId={headingId}
        title={ORGANIZATIONS_TITLE}
        description={ORGANIZATIONS_DESCRIPTION}
        actionLabel="Create organization"
        onAction={() => setCreating(true)}
        disabledReason={null}
      />
      <DirectorySearchForm
        label="Search organizations"
        placeholder="Organization name or slug"
        value={searchDraft}
        inputRef={searchRef}
        onChange={setSearchDraft}
        onSubmit={applySearch}
      />
      {body}
      {creating ? (
        <CreateOrganizationDialog
          client={client}
          parents={picker.page}
          parentsFailed={picker.failed}
          onClose={() => setCreating(false)}
          onCreated={(created) => {
            setCreating(false);
            setToast(`Created ${created.name}.`);
            setPageNumber(1);
            directory.reload();
            setPickerGeneration((value) => value + 1);
          }}
        />
      ) : null}
      <DirectoryToast message={toast} onDismiss={() => setToast(null)} />
    </section>
  );
}

function OrganizationsTableHead() {
  return (
    <thead>
      <tr>
        <th scope="col">Organization</th>
        <th scope="col">Type</th>
        <th scope="col">Hierarchy</th>
        <th scope="col">Description</th>
        <th scope="col">Created</th>
      </tr>
    </thead>
  );
}

function OrganizationRow({ organization, names }: {
  readonly organization: IdentityOrganization;
  readonly names: ReadonlyMap<string, string>;
}) {
  const kind = organizationKind(organization);
  const ancestry = organizationAncestry(organization, names);
  return (
    <tr data-organization-id={organization.id}>
      <td data-label="Organization">
        <strong className="site-directory-name">{organization.name}</strong>
        <small className="table-secondary"><code>{organization.slug}</code> · ID {organization.id}</small>
      </td>
      <td data-label="Type">
        <span className={KIND_BADGE[kind]}>{ORGANIZATION_KIND_LABELS[kind]}</span>
      </td>
      <td data-label="Hierarchy">
        {ancestry.length === 0 ? (
          <span className="site-directory-muted">Root organization</span>
        ) : (
          <ol className="site-directory-lineage" aria-label={`Ancestors of ${organization.name}`}>
            {ancestry.map((ancestor) => <li key={ancestor.id}>{ancestor.name}</li>)}
            <li aria-current="true">{organization.name}</li>
          </ol>
        )}
      </td>
      <td data-label="Description" className="site-directory-description">
        {organization.description === undefined || organization.description.length === 0
          ? <span className="site-directory-muted">No description</span>
          : organization.description}
      </td>
      <td data-label="Created">{formatMediumDateTime(new Date(organization.createdAt), "Unknown")}</td>
    </tr>
  );
}

const ORGANIZATION_FIELD_IDS = {
  description: "site-orgs-create-description",
  name: "site-orgs-create-name",
  parent: "site-orgs-create-parent",
  slug: "site-orgs-create-slug",
} as const;

const TOP_LEVEL_VALUE = "__top_level__";

function CreateOrganizationDialog({ client, parents, parentsFailed, onClose, onCreated }: {
  readonly client: IdentityClient;
  readonly parents: Page<IdentityOrganization> | null;
  readonly parentsFailed: boolean;
  readonly onClose: () => void;
  readonly onCreated: (created: IdentityOrganization) => void;
}) {
  const [draft, setDraft] = useState<CreateOrganizationDraft>(EMPTY_CREATE_ORGANIZATION_DRAFT);
  const [attempted, setAttempted] = useState(false);
  const [pending, setPending] = useState(false);
  const [serverError, setServerError] = useState<IdentityError | null>(null);
  const formId = useId();

  const clientErrors = attempted ? validateCreateOrganizationDraft(draft) : {};
  const serverField = serverError === null ? null : createOrganizationErrorField(serverError);
  const errors: CreateOrganizationErrors = serverField === null || serverError === null
    ? clientErrors
    : { ...clientErrors, [serverField]: clientErrors[serverField] ?? serverError.message };
  const formMessage = serverError !== null && serverField === null
    ? presentMutationError(serverError)
    : attempted && hasErrors(clientErrors) ? "Fix the highlighted fields, then create the organization again." : null;

  function update(next: CreateOrganizationDraft) {
    setDraft(next);
    setServerError(null);
  }

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (pending) return;
    setAttempted(true);
    if (hasErrors(validateCreateOrganizationDraft(draft))) return;
    setPending(true);
    setServerError(null);
    try {
      onCreated(await client.adminCreateOrganization(createOrganizationInput(draft)));
    } catch (caught) {
      setServerError(asIdentityError(caught));
      setPending(false);
    }
  }

  const parentOptions = (parents?.items ?? []).filter((organization) => organization.personalOwnerUserId === undefined);
  const parentHint = parentsFailed
    ? "The organization list could not be loaded, so only a top-level organization can be created right now."
    : parents === null
      ? "Loading organizations…"
      : parents.page.hasNextPage
        ? `Showing the first ${parents.items.length} organizations. A child inherits its parent's roles.`
        : "A child organization inherits its parent's member roles.";

  return (
    <Modal
      title="Create organization"
      subtitle="A new tenant with its own indexes, searches and members. You can add members once it exists."
      dismissible={!pending}
      initialFocus={`#${ORGANIZATION_FIELD_IDS.name}`}
      onClose={onClose}
      footer={(
        <>
          <button className="button button--secondary" type="button" disabled={pending} onClick={onClose}>Cancel</button>
          <button className="button button--primary" type="submit" form={formId} aria-disabled={pending ? true : undefined}>
            {pending ? "Creating…" : "Create organization"}
          </button>
        </>
      )}
    >
      <form className="admin-form" id={formId} noValidate onSubmit={(event) => void submit(event)} aria-busy={pending}>
        <DirectoryFormError message={formMessage} />
        <label htmlFor={ORGANIZATION_FIELD_IDS.name}>
          <span>Name</span>
          <input
            id={ORGANIZATION_FIELD_IDS.name}
            autoComplete="off"
            required
            value={draft.name}
            disabled={pending}
            onChange={(event) => update(withOrganizationName(draft, event.target.value))}
            {...fieldControlProps(ORGANIZATION_FIELD_IDS.name, errors.name ?? null)}
          />
          <FieldNote fieldId={ORGANIZATION_FIELD_IDS.name} error={errors.name ?? null}>Shown in the organization switcher.</FieldNote>
        </label>
        <label htmlFor={ORGANIZATION_FIELD_IDS.slug}>
          <span>Slug</span>
          <input
            id={ORGANIZATION_FIELD_IDS.slug}
            autoComplete="off"
            required
            value={draft.slug}
            disabled={pending}
            onChange={(event) => update({ ...draft, slug: event.target.value, slugEdited: true })}
            {...fieldControlProps(ORGANIZATION_FIELD_IDS.slug, errors.slug ?? null)}
          />
          <FieldNote fieldId={ORGANIZATION_FIELD_IDS.slug} error={errors.slug ?? null}>Lowercase letters and digits joined by dashes; suggested from the name.</FieldNote>
        </label>
        <label htmlFor={ORGANIZATION_FIELD_IDS.parent}>
          <span>Parent organization</span>
          <Select
            id={ORGANIZATION_FIELD_IDS.parent}
            value={draft.parentId === "" ? TOP_LEVEL_VALUE : draft.parentId}
            disabled={pending || parents === null}
            aria-describedby={`${ORGANIZATION_FIELD_IDS.parent}-note`}
            onValueChange={(value) => update({ ...draft, parentId: value === TOP_LEVEL_VALUE ? "" : value })}
          >
            <SelectOption value={TOP_LEVEL_VALUE}>None (top level)</SelectOption>
            {parentOptions.map((organization) => (
              <SelectOption key={organization.id} value={organization.id}>{organization.name}</SelectOption>
            ))}
          </Select>
          <small id={`${ORGANIZATION_FIELD_IDS.parent}-note`}>{parentHint}</small>
        </label>
        <label htmlFor={ORGANIZATION_FIELD_IDS.description}>
          <span>Description (optional)</span>
          <textarea
            id={ORGANIZATION_FIELD_IDS.description}
            value={draft.description}
            disabled={pending}
            onChange={(event) => update({ ...draft, description: event.target.value })}
            {...fieldControlProps(ORGANIZATION_FIELD_IDS.description, errors.description ?? null)}
          />
          <FieldNote fieldId={ORGANIZATION_FIELD_IDS.description} error={errors.description ?? null}>Up to 500 characters.</FieldNote>
        </label>
      </form>
    </Modal>
  );
}
