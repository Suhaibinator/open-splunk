package main

import (
	"errors"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestNativeRetainedKnowledgeUsesCurrentPrivateSharingAndOrganization(t *testing.T) {
	database := pgtest.Open(t)
	ctx := t.Context()
	for _, statement := range []string{
		`CREATE TABLE app_workspaces(tenant_id TEXT,organization_id BIGINT,app_id TEXT,state TEXT)`,
		`CREATE TABLE knowledge_objects(tenant_id TEXT,organization_id BIGINT,knowledge_object_id TEXT,owner_id TEXT,app_id TEXT,sharing_scope TEXT,state TEXT)`,
		`CREATE TABLE knowledge_lookup_definitions(tenant_id TEXT,organization_id BIGINT,lookup_id TEXT,owner_id TEXT,app_id TEXT,sharing_scope BIGINT,state TEXT)`,
		`INSERT INTO app_workspaces VALUES ('physical',1,'local','active'),('physical',1,'global','active'),('physical',2,'foreign','active'),('physical',1,'archived','archived')`,
		`INSERT INTO knowledge_objects VALUES
		 ('physical',1,'own','42','local','private','active'),('physical',1,'other','99','local','private','active'),
		 ('physical',1,'shared','99','local','app','active'),('physical',1,'global','99','global','global','active'),
		 ('physical',2,'foreign','42','foreign','private','active'),('physical',1,'inactive','42','local','private','disabled'),
		 ('physical',1,'archived','42','archived','global','active')`,
		`INSERT INTO knowledge_lookup_definitions VALUES
		 ('physical',1,'own','42','local',1,'ACTIVE'),('physical',1,'other','99','local',1,'ACTIVE'),
		 ('physical',1,'shared','99','local',2,'ACTIVE'),('physical',1,'global','99','global',3,'ACTIVE'),
		 ('physical',2,'foreign','42','foreign',1,'ACTIVE'),('physical',1,'inactive','42','local',1,'DISABLED'),
		 ('physical',1,'archived','42','archived',3,'ACTIVE')`,
	} {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	access := searchjobs.AccessScope{TenantID: "physical", OwnerID: "42", OrganizationID: "1"}
	for _, test := range []struct {
		name    string
		ids     []string
		allowed bool
	}{
		{"owned private", []string{"own"}, true},
		{"shared current organization", []string{"own", "shared", "global"}, true},
		{"other private after demotion", []string{"other"}, false},
		{"foreign organization", []string{"foreign"}, false},
		{"disabled source", []string{"inactive"}, false},
		{"archived source app", []string{"archived"}, false},
		{"missing source", []string{"missing"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, lookup := range []bool{false, true} {
				var objects, lookups []string
				if lookup {
					lookups = test.ids
				} else {
					objects = test.ids
				}
				err := authorizeRuntimeSourceRegistries(ctx, database, access, 1, "local", objects, lookups)
				if test.allowed && err != nil {
					t.Fatalf("current visible sources denied (lookup=%t): %v", lookup, err)
				}
				if !test.allowed && !errors.Is(err, authorization.ErrForbidden) {
					t.Fatalf("invisible source allowed (lookup=%t): %v", lookup, err)
				}
			}
		})
	}
	access.SiteAdministrator = true
	if err := authorizeRuntimeSourceRegistries(ctx, database, access, 1, "local", []string{"other", "foreign", "missing"}, []string{"other", "foreign", "missing"}); err != nil {
		t.Fatalf("site administrator source override: %v", err)
	}
}
