package main

import (
	"fmt"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Exercise each registry independently so the deletion guard does not depend
// on a different resource (such as its app) incidentally blocking deletion.
func TestOrganizationDeletionRejectsKnowledgeDependencies(t *testing.T) {
	for _, registry := range []string{"knowledge_objects", "knowledge_lookup_definitions"} {
		for _, organizationID := range []int64{1, 2} {
			t.Run(fmt.Sprintf("%s/organization-%d", registry, organizationID), func(t *testing.T) {
				pool := pgtest.Open(t)
				orm, err := gorm.Open(postgres.New(postgres.Config{Conn: pool}), &gorm.Config{})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := pool.ExecContext(t.Context(), `CREATE TABLE gocommon_organizations(id bigint PRIMARY KEY,lineage bigint[] NOT NULL);
INSERT INTO gocommon_organizations VALUES(1,'{}'),(2,'{1}'),(3,'{}')`); err != nil {
					t.Fatal(err)
				}
				for _, table := range []string{
					"os_index_organizations", "app_workspaces", "saved_searches", "dashboards",
					"knowledge_objects", "knowledge_lookup_definitions", "search_capture_sessions",
					"search_retained_export_reservations", "durable_search_jobs", "durable_export_jobs",
					"search_history", "search_history_pending", "saved_search_schedules",
					"saved_search_schedule_runs", "alerts", "ingestion_tokens",
				} {
					if _, err := pool.ExecContext(t.Context(), "CREATE TABLE "+table+"(organization_id bigint NOT NULL REFERENCES gocommon_organizations(id))"); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := pool.ExecContext(t.Context(), map[string]string{"knowledge_objects": "INSERT INTO knowledge_objects(organization_id)VALUES($1)", "knowledge_lookup_definitions": "INSERT INTO knowledge_lookup_definitions(organization_id)VALUES($1)"}[registry], organizationID); err != nil {
					t.Fatal(err)
				}
				tx := orm.WithContext(t.Context()).Begin()
				if tx.Error != nil {
					t.Fatal(tx.Error)
				}
				defer tx.Rollback()
				if err := beforeRuntimeOrganizationDelete(t.Context(), tx, 1); err == nil || err.Error() != "organization contains OpenSplunk resources" {
					t.Fatalf("deletion with %s resource in organization %d: %v", registry, organizationID, err)
				}
				if err := beforeRuntimeOrganizationDelete(t.Context(), tx, 3); err != nil {
					t.Fatalf("unrelated empty organization deletion: %v", err)
				}
			})
		}
	}
}
