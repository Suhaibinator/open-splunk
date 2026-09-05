import { expect, test, type Page } from "@playwright/test";

import { AppState } from "../../gen/ts/open_splunk/app";
import { CreateAppResponse } from "../../gen/ts/open_splunk/app_api";
import { SharingScope } from "../../gen/ts/open_splunk/common";
import type { Dashboard } from "../../gen/ts/open_splunk/dashboard";
import { CreateDashboardRequest, CreateDashboardResponse, DeleteDashboardResponse, ListDashboardsRequest, ListDashboardsResponse } from "../../gen/ts/open_splunk/dashboard_api";
import { GetSystemBootstrapRequest, GetSystemBootstrapResponse, ServerFeature } from "../../gen/ts/open_splunk/system_api";

const protobufHeaders = { "content-type": "application/x-protobuf" };

function app(appId: string, displayName: string) {
  return { appId, slug: displayName.toLowerCase(), displayName, defaultIndexNames: [], state: AppState.APP_STATE_ACTIVE };
}

async function mockBootstrap(page: Page, apps = [app("app-1", "GradeThis")], selectedAppId = apps[0]?.appId ?? "") {
  await page.route("**/api/system/bootstrap", (route) => route.fulfill({
    status: 200,
    headers: protobufHeaders,
    body: Buffer.from(GetSystemBootstrapResponse.encode(GetSystemBootstrapResponse.fromPartial({
      apps,
      selectedAppId,
      serverTime: new Date("2026-09-05T00:00:00Z"),
      features: [ServerFeature.SERVER_FEATURE_DASHBOARDS, ServerFeature.SERVER_FEATURE_APP_ADMIN],
    })).finish()),
  }));
}

async function mockDashboardList(page: Page, dashboards: Dashboard[]) {
  await page.route("**/api/dashboards/list", (route) => route.fulfill({
    status: 200,
    headers: protobufHeaders,
    body: Buffer.from(ListDashboardsResponse.encode({ dashboards }).finish()),
  }));
}

function dashboard(dashboardId: string, appId: string, name: string): Dashboard {
  return {
    dashboardId,
    version: 1n,
    definition: { name, appId, sharingScope: SharingScope.SHARING_SCOPE_PRIVATE, panels: [] },
    createdAt: new Date("2026-09-05T00:00:00Z"),
    updatedAt: new Date("2026-09-05T00:00:00Z"),
  };
}

test("creates the first dashboard from the single onboarding card", async ({ page }) => {
  await mockBootstrap(page);
  await mockDashboardList(page, []);
  await page.route("**/api/dashboards/create", async (route) => {
    const wire = route.request().postDataBuffer();
    if (wire === null) throw new Error("dashboard create omitted its protobuf body");
    const request = CreateDashboardRequest.decode(wire);
    await route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(CreateDashboardResponse.encode({ dashboard: dashboard("dash-1", "app-1", request.definition?.name ?? "") }).finish()),
    });
  });

  await page.goto("/dashboards/");
  await expect(page.getByRole("heading", { name: "Create your first dashboard" })).toBeVisible();
  await page.screenshot({ path: "/private/tmp/open-splunk-dashboard-empty.png", fullPage: true });
  await expect(page.getByText("Select a dashboard")).toHaveCount(0);
  await page.getByLabel("Dashboard name").fill("Service overview");
  await page.getByRole("button", { name: "Create dashboard" }).click();
  await expect(page.getByRole("heading", { name: "Dashboard settings" })).toBeVisible();
  await expect(page.getByText("Service overview", { exact: true })).toBeVisible();
  await page.screenshot({ path: "/private/tmp/open-splunk-dashboard-editor.png", fullPage: true });
});

test("creates the prerequisite app and continues into dashboard onboarding", async ({ page }) => {
  let created = false;
  await page.route("**/api/system/bootstrap", (route) => route.fulfill({
    status: 200,
    headers: protobufHeaders,
    body: Buffer.from(GetSystemBootstrapResponse.encode(GetSystemBootstrapResponse.fromPartial({
      apps: created ? [app("app-new", "New App")] : [],
      selectedAppId: created ? "app-new" : "",
      serverTime: new Date("2026-09-05T00:00:00Z"),
      features: [ServerFeature.SERVER_FEATURE_DASHBOARDS, ServerFeature.SERVER_FEATURE_APP_ADMIN],
    })).finish()),
  }));
  await mockDashboardList(page, []);
  await page.route("**/api/apps/create", async (route) => {
    created = true;
    await route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(CreateAppResponse.encode(CreateAppResponse.fromPartial({
        app: { appId: "app-new", state: AppState.APP_STATE_ACTIVE, version: 1n, definition: { slug: "new-app", displayName: "New App", defaultIndexNames: [] } },
      })).finish()),
    });
  });

  await page.goto("/dashboards/");
  await expect(page.getByRole("heading", { name: "Create an app workspace first" })).toBeVisible();
  await page.screenshot({ path: "/private/tmp/open-splunk-dashboard-no-app.png", fullPage: true });
  await page.getByRole("button", { name: "Create app" }).click();
  await page.getByLabel("Slug").fill("new-app");
  await page.getByLabel("Display name").fill("New App");
  await page.getByRole("button", { name: "Create app" }).last().click();
  await expect(page.getByRole("heading", { name: "Create your first dashboard" })).toBeVisible();
  await expect(page).toHaveURL(/appId=app-new/u);
});

test("a failed dashboard load never claims the catalog is empty", async ({ page }) => {
  await mockBootstrap(page);
  await page.route("**/api/dashboards/list", (route) => route.fulfill({ status: 500, body: "failed" }));
  await page.goto("/dashboards/");
  await expect(page.locator(".operations-error-banner[role=alert]")).toBeVisible();
  await expect(page.getByRole("button", { name: "Retry" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Create your first dashboard" })).toHaveCount(0);
});

test("deleting the last dashboard returns to the creation card", async ({ page }) => {
  await mockBootstrap(page);
  await mockDashboardList(page, [dashboard("dash-1", "app-1", "Operations")]);
  await page.route("**/api/dashboards/delete", (route) => route.fulfill({
    status: 200,
    headers: protobufHeaders,
    body: Buffer.from(DeleteDashboardResponse.encode(DeleteDashboardResponse.fromPartial({})).finish()),
  }));
  await page.goto("/dashboards/");
  await expect(page.getByRole("button", { name: "Add panel", exact: true })).toBeDisabled();
  await expect(page.getByText("No searchable indexes are available. Add one before creating panels.")).toBeVisible();
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "Delete", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Create your first dashboard" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Saved dashboards" })).toHaveCount(0);
});

test("unsupported dashboards show an explanation instead of perpetual loading", async ({ page }) => {
  await page.route("**/api/system/bootstrap", (route) => route.fulfill({
    status: 200,
    headers: protobufHeaders,
    body: Buffer.from(GetSystemBootstrapResponse.encode(GetSystemBootstrapResponse.fromPartial({
      serverTime: new Date("2026-09-05T00:00:00Z"),
      apps: [app("app-1", "GradeThis")],
      selectedAppId: "app-1",
      features: [],
    })).finish()),
  }));
  await page.goto("/dashboards/");
  await expect(page.getByRole("heading", { name: "Dashboards unavailable" })).toBeVisible();
  await expect(page.getByText("Loading dashboards…")).toHaveCount(0);
});

test("cancelling an app switch preserves the dirty dashboard and URL", async ({ page }) => {
  const apps = [app("app-1", "GradeThis"), app("app-2", "Security")];
  await mockBootstrap(page, apps, "app-1");
  await mockDashboardList(page, [dashboard("dash-1", "app-1", "Operations")]);
  await page.goto("/dashboards/?appId=app-1");
  await page.getByLabel("Name", { exact: true }).fill("Unsaved name");
  page.once("dialog", (dialog) => dialog.dismiss());
  await page.getByRole("button", { name: /App:/u }).click();
  await page.getByRole("menuitem", { name: "Select Security" }).click();
  await expect(page.getByLabel("Name", { exact: true })).toHaveValue("Unsaved name");
  await expect(page).toHaveURL(/appId=app-1/u);
});

test("a rejected app switch preserves the loaded dirty dashboard", async ({ page }) => {
  const apps = [app("app-1", "GradeThis"), app("app-2", "Security")];
  await mockBootstrap(page, apps, "app-1");
  await mockDashboardList(page, [dashboard("dash-1", "app-1", "Operations")]);
  await page.goto("/dashboards/?appId=app-1");
  await page.getByLabel("Name", { exact: true }).fill("Unsaved name");
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: /App:/u }).click();
  await page.getByRole("menuitem", { name: "Select Security" }).click();
  await expect(page.locator(".operations-error-banner[role=alert]")).toContainText("no longer available");
  await expect(page.getByLabel("Name", { exact: true })).toHaveValue("Unsaved name");
  await expect(page).toHaveURL(/appId=app-1/u);
});

test("app creation failure keeps the setup dialog actionable", async ({ page }) => {
  await mockBootstrap(page, [], "");
  await mockDashboardList(page, []);
  await page.route("**/api/apps/create", (route) => route.fulfill({ status: 500, body: "failed" }));
  await page.goto("/dashboards/");
  await page.getByRole("button", { name: "Create app" }).click();
  await page.getByLabel("Slug").fill("new-app");
  await page.getByLabel("Display name").fill("New App");
  await page.getByRole("button", { name: "Create app" }).last().click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await expect(page.getByRole("dialog").getByRole("alert")).toContainText("could not be created");
  await expect(page.getByLabel("Display name")).toHaveValue("New App");
});

test("bootstrap failure offers Retry without showing an empty state", async ({ page }) => {
  let failing = true;
  await page.route("**/api/system/bootstrap", (route) => {
    if (failing) return route.fulfill({ status: 500, body: "failed" });
    return route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(GetSystemBootstrapResponse.encode(GetSystemBootstrapResponse.fromPartial({
        apps: [app("app-1", "GradeThis")],
        selectedAppId: "app-1",
        serverTime: new Date("2026-09-05T00:00:00Z"),
        features: [ServerFeature.SERVER_FEATURE_DASHBOARDS],
      })).finish()),
    });
  });
  await mockDashboardList(page, []);
  await page.goto("/dashboards/");
  await expect(page.locator(".operations-error-banner[role=alert]")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Create your first dashboard" })).toHaveCount(0);
  failing = false;
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Create your first dashboard" })).toBeVisible();
});

test("no accessible apps without administration explains recovery", async ({ page }) => {
  await page.route("**/api/system/bootstrap", (route) => route.fulfill({
    status: 200,
    headers: protobufHeaders,
    body: Buffer.from(GetSystemBootstrapResponse.encode(GetSystemBootstrapResponse.fromPartial({
      apps: [],
      selectedAppId: "",
      serverTime: new Date("2026-09-05T00:00:00Z"),
      features: [ServerFeature.SERVER_FEATURE_DASHBOARDS],
    })).finish()),
  }));
  await page.goto("/dashboards/");
  await expect(page.getByRole("heading", { name: "Create an app workspace first" })).toBeVisible();
  await expect(page.getByText(/ask an administrator to give you access/u)).toBeVisible();
  await expect(page.getByText(/Create one here/u)).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Check again" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Create app" })).toHaveCount(0);
});

test("rapid app selections commit only the latest successful catalog", async ({ page }) => {
  const apps = [app("app-1", "GradeThis"), app("app-2", "Security"), app("app-3", "Billing")];
  await page.route("**/api/system/bootstrap", async (route) => {
    const wire = route.request().postDataBuffer();
    const preferred = wire === null ? undefined : GetSystemBootstrapRequest.decode(wire).preferredAppId;
    if (preferred === "app-2") await new Promise((resolve) => setTimeout(resolve, 150));
    if (preferred === "app-3") await new Promise((resolve) => setTimeout(resolve, 10));
    const selectedAppId = preferred ?? "app-1";
    await route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(GetSystemBootstrapResponse.encode(GetSystemBootstrapResponse.fromPartial({
        apps,
        selectedAppId,
        serverTime: new Date("2026-09-05T00:00:00Z"),
        features: [ServerFeature.SERVER_FEATURE_DASHBOARDS],
      })).finish()),
    });
  });
  await page.route("**/api/dashboards/list", async (route) => {
    const wire = route.request().postDataBuffer();
    if (wire === null) throw new Error("dashboard list omitted its protobuf body");
    const appId = ListDashboardsRequest.decode(wire).appIdFilter ?? "app-1";
    await route.fulfill({
      status: 200,
      headers: protobufHeaders,
      body: Buffer.from(ListDashboardsResponse.encode({ dashboards: [dashboard(`dash-${appId}`, appId, `${appId} dashboard`)] }).finish()),
    });
  });
  await page.goto("/dashboards/?appId=app-1");
  await page.getByRole("button", { name: /App:/u }).click();
  await page.getByRole("menuitem", { name: "Select Security" }).click();
  await page.getByRole("button", { name: /App:/u }).click();
  await page.getByRole("menuitem", { name: "Select Billing" }).click();
  await expect(page.getByRole("button", { name: /App: Billing/u })).toBeVisible();
  await expect(page.getByText("app-3 dashboard", { exact: true })).toBeVisible();
  await expect(page).toHaveURL(/appId=app-3/u);
});

for (const theme of ["light", "dark"] as const) {
  for (const width of [980, 760, 480, 390]) {
    test(`dashboard onboarding stays contained at ${width}px in ${theme} theme`, async ({ page }) => {
      await mockBootstrap(page);
      await mockDashboardList(page, []);
      await page.addInitScript(([key, value]) => window.localStorage.setItem(key, value), ["open-splunk.theme", theme]);
      await page.setViewportSize({ width, height: 760 });
      await page.goto("/dashboards/");
      const card = page.locator(".operations-empty-dashboard");
      await expect(card).toBeVisible();
      const bounds = await card.boundingBox();
      expect(bounds).not.toBeNull();
      expect(bounds!.x).toBeGreaterThanOrEqual(0);
      expect(bounds!.x + bounds!.width).toBeLessThanOrEqual(width);
      await page.screenshot({ path: `/private/tmp/open-splunk-dashboard-${theme}-${width}.png`, fullPage: true });
    });
  }
}
