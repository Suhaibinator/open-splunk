import assert from "node:assert/strict";
import test from "node:test";
import { GoCommonClient, go_common } from "@suhaibinator/go-common-core";
import { currentIdentity, clearIdentitySession } from "./identity-session";
import { finishGoogleSignIn, localReturnUrl, refreshIdentity } from "./identity-client";

test.afterEach(clearIdentitySession);
test("Google registration resolves a user with no team organization membership", async () => {
  const requests: string[] = [];
  const client = new GoCommonClient({ baseUrl: "https://identity.example", apiPrefix: "/api/identity", fetch: async (url, options) => {
    assert.equal(options?.credentials, "include");
    requests.push(String(url));
    if (String(url).endsWith("/organizations/list")) {
      const request = go_common.v1.ListOrganizationsRequest.decode(new Uint8Array(options?.body as ArrayBuffer));
      assert.equal(request.organizationTypeFilter, go_common.v1.OrganizationType.ORGANIZATION_TYPE_BUSINESS_TIER_0);
    }
    const body = String(url).endsWith("/user/me")
      ? go_common.v1.GetCurrentUserResponse.encode(go_common.v1.GetCurrentUserResponse.fromPartial({ user: { id: 7n, email: "new@example.com", userType: 1 } })).finish()
      : go_common.v1.ListOrganizationsResponse.encode(go_common.v1.ListOrganizationsResponse.fromPartial({ organizations: [], pagination: { hasNextPage: false } })).finish();
    return new Response(body.slice().buffer, { headers: { "Content-Type": "application/x-protobuf" } });
  } });
  await refreshIdentity(client);
  assert.equal(currentIdentity().status, "authenticated");
  assert.equal(currentIdentity().user?.id, 7n);
  assert.equal(currentIdentity().organizationId, null);
  assert.deepEqual(currentIdentity().organizations, []);
  assert.deepEqual(requests, ["https://identity.example/api/identity/user/me", "https://identity.example/api/identity/organizations/list"]);
});

test("OAuth callback remounts share one native code exchange", async () => {
  let exchanges = 0;
  const client = new GoCommonClient({ baseUrl: "https://identity.example", apiPrefix: "/api/identity", fetch: async (url) => {
    let body: Uint8Array;
    if (String(url).endsWith("/auth/oauth/callback")) { exchanges += 1; body = go_common.v1.OAuthTokenResponse.encode(go_common.v1.OAuthTokenResponse.fromPartial({ returnUrl: "/search/events/", providerSlug: "google" })).finish(); }
    else if (String(url).endsWith("/user/me")) body = go_common.v1.GetCurrentUserResponse.encode(go_common.v1.GetCurrentUserResponse.fromPartial({ user: { id: 8n, email: "new@example.com", userType: 1 } })).finish();
    else body = go_common.v1.ListOrganizationsResponse.encode(go_common.v1.ListOrganizationsResponse.fromPartial({ organizations: [] })).finish();
    return new Response(body.slice().buffer, { headers: { "Content-Type": "application/x-protobuf" } });
  } });
  const responses = await Promise.all([finishGoogleSignIn(client, "code", "test-state"), finishGoogleSignIn(client, "code", "test-state")]);
  assert.deepEqual(responses, ["/search/events/", "/search/events/"]);
  assert.equal(exchanges, 1);
});

test("login return destinations remain local", () => {
  for (const value of ["https://external.example", "//external.example", "/\\external.example", "/\t/external.example"]) assert.equal(localReturnUrl(value), "/");
  assert.equal(localReturnUrl("/search/events/?appId=one"), "/search/events/?appId=one");
});

test("site administrators list only team organization types", async () => {
  const client = new GoCommonClient({baseUrl:"https://identity.example",fetch:async(url,options)=>{
    const profile=String(url).endsWith("/user/me");
    if (!profile) {
      assert.ok(String(url).endsWith("/admin/organizations/list-all"));
      const request=go_common.v1.AdminListAllOrganizationsRequest.decode(new Uint8Array(options?.body as ArrayBuffer));
      assert.equal(request.organizationTypeFilter,go_common.v1.OrganizationType.ORGANIZATION_TYPE_BUSINESS_TIER_0);
    }
    const body=profile
      ? go_common.v1.GetCurrentUserResponse.encode(go_common.v1.GetCurrentUserResponse.fromPartial({user:{id:77n,email:"site@example.com",userType:44}})).finish()
      : go_common.v1.AdminListAllOrganizationsResponse.encode(go_common.v1.AdminListAllOrganizationsResponse.fromPartial({organizations:[]})).finish();
    return new Response(body.slice().buffer,{headers:{"Content-Type":"application/x-protobuf"}});
  }});
  await refreshIdentity(client);
  assert.equal(currentIdentity().status,"authenticated");
  assert.deepEqual(currentIdentity().organizations,[]);
});
