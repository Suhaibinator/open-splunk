package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	internalclickhouse "github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

type physicalIndexAuthority struct {
	tenant, name string
	maximum      uint64
}

// A new control database must never adopt existing event rows by recreating
// names or visibility counters. Pending preserved outboxes drain first so a
// normal crash between a ClickHouse insert and SQL commit remains recoverable.
func validateRuntimePreservation(ctx context.Context, db *control.DB, events *internalclickhouse.Store, connection clickhousedriver.Conn, tenant string) error {
	return events.WithWritesFrozen(ctx, func(ctx context.Context, frozen internalclickhouse.FrozenWrites) error {
		if err := frozen.DrainPending(ctx); err != nil {
			return fmt.Errorf("drain preserved visibility authority: %w", err)
		}
		var assigned, committed int64
		if err := db.SQLDB().QueryRowContext(ctx, "SELECT last_assigned,committed_through FROM ingest_visibility_state WHERE singleton=1").Scan(&assigned, &committed); err != nil {
			return err
		}
		rows, err := db.SQLDB().QueryContext(ctx, `SELECT i.name FROM indexes i JOIN os_index_organizations b ON b.index_id=i.index_id JOIN gocommon_organizations o ON o.id=b.organization_id AND o.deleted_at IS NULL ORDER BY i.name LIMIT $1`, control.MaximumPhysicalIndexRecords+1)
		if err != nil {
			return err
		}
		known := make(map[string]bool)
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return errors.Join(err, rows.Close())
			}
			known[name] = true
		}
		scanErr := rows.Err()
		closeErr := rows.Close()
		if scanErr != nil {
			return scanErr
		}
		if closeErr != nil {
			return closeErr
		}
		if len(known) > control.MaximumPhysicalIndexRecords {
			return fmt.Errorf("preserved index catalog exceeds its bound")
		}
		if err := compareRuntimePreservedAuthority(tenant, assigned, committed, known, nil); err != nil {
			return err
		}
		names := make([]string, 0, len(known))
		for name := range known {
			names = append(names, name)
		}
		sort.Strings(names)
		committedUnsigned, err := strconv.ParseUint(strconv.FormatInt(committed, 10), 10, 64)
		if err != nil {
			return err
		}
		args := []any{tenant, committedUnsigned}
		unknownIndexPredicate := "1"
		if len(names) > 0 {
			unknownIndexPredicate = "index_name NOT IN (" + strings.TrimSuffix(strings.Repeat("?,", len(names)), ",") + ")"
			for _, name := range names {
				args = append(args, name)
			}
		}
		// Healthy startup prunes known tenant/index primary-key ranges and every
		// visibility granule using the existing minmax index. Read only mismatches.
		query := "SELECT tenant_id,index_name,max(visibility_seq) FROM open_splunk.events WHERE tenant_id<>? OR visibility_seq>? OR " + unknownIndexPredicate + " GROUP BY tenant_id,index_name ORDER BY tenant_id,index_name LIMIT 1025"
		physicalRows, err := connection.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("inspect existing ClickHouse event authority: %w", err)
		}
		physical := make([]physicalIndexAuthority, 0)
		for physicalRows.Next() {
			var item physicalIndexAuthority
			if err := physicalRows.Scan(&item.tenant, &item.name, &item.maximum); err != nil {
				return errors.Join(err, physicalRows.Close())
			}
			physical = append(physical, item)
		}
		physicalErr := physicalRows.Err()
		physicalCloseErr := physicalRows.Close()
		if physicalErr != nil {
			return physicalErr
		}
		if physicalCloseErr != nil {
			return physicalCloseErr
		}
		return compareRuntimePreservedAuthority(tenant, assigned, committed, known, physical)
	})
}

func compareRuntimePreservedAuthority(tenant string, assigned, committed int64, known map[string]bool, physical []physicalIndexAuthority) error {
	preservation := func(reason string) error {
		return fmt.Errorf("existing ClickHouse data requires preserved PostgreSQL control state before startup: %s; run the separate preservation utility and retain physical tenant/index/visibility identities", reason)
	}
	if assigned < 0 || committed < 0 || committed > assigned {
		return preservation("visibility state is invalid")
	}
	if len(physical) > control.MaximumPhysicalIndexRecords {
		return preservation("physical index authority exceeds the bounded catalog")
	}
	for _, item := range physical {
		if item.tenant != tenant {
			return preservation("configured physical tenant differs from existing events")
		}
		if !known[item.name] {
			return preservation("physical index is absent from the owned preserved catalog")
		}
		if item.maximum > uint64(assigned) || item.maximum > uint64(committed) {
			return preservation("event visibility exceeds preserved committed authority")
		}
	}
	return nil
}
