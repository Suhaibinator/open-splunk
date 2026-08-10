package auth

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestRevalidateHECAdmissionInTransactionRequiresExactCurrentAuthority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(
			*testing.T,
			context.Context,
			*control.DB,
			control.Index,
			control.Index,
			*HECAdmissionAuthority,
			time.Time,
		)
		want error
	}{
		{name: "exact snapshot"},
		{
			name: "token version changed",
			mutate: func(
				t *testing.T,
				ctx context.Context,
				db *control.DB,
				_ control.Index,
				_ control.Index,
				authority *HECAdmissionAuthority,
				_ time.Time,
			) {
				t.Helper()
				if _, err := db.SQLDB().ExecContext(ctx, `
					UPDATE ingestion_tokens
					SET version = version + 1
					WHERE ingestion_token_id = ?`, authority.TokenID); err != nil {
					t.Fatalf("advance HEC token version: %v", err)
				}
			},
			want: ErrStaleHECAdmission,
		},
		{
			name: "token disabled",
			mutate: func(
				t *testing.T,
				ctx context.Context,
				db *control.DB,
				_ control.Index,
				_ control.Index,
				authority *HECAdmissionAuthority,
				_ time.Time,
			) {
				t.Helper()
				if _, err := db.SQLDB().ExecContext(ctx, `
					UPDATE ingestion_tokens
					SET state = 'disabled'
					WHERE ingestion_token_id = ?`, authority.TokenID); err != nil {
					t.Fatalf("disable HEC token: %v", err)
				}
			},
			want: ErrStaleHECAdmission,
		},
		{
			name: "token expired",
			mutate: func(
				t *testing.T,
				ctx context.Context,
				db *control.DB,
				_ control.Index,
				_ control.Index,
				authority *HECAdmissionAuthority,
				checkedAt time.Time,
			) {
				t.Helper()
				if _, err := db.SQLDB().ExecContext(ctx, `
					UPDATE ingestion_tokens
					SET expires_at_unix_micro = ?
					WHERE ingestion_token_id = ?`,
					checkedAt.UnixMicro(),
					authority.TokenID,
				); err != nil {
					t.Fatalf("expire HEC token: %v", err)
				}
			},
			want: ErrStaleHECAdmission,
		},
		{
			name: "acknowledgment mode mismatch",
			mutate: func(
				_ *testing.T,
				_ context.Context,
				_ *control.DB,
				_ control.Index,
				_ control.Index,
				authority *HECAdmissionAuthority,
				_ time.Time,
			) {
				authority.IndexerAcknowledgment = false
			},
			want: ErrStaleHECAdmission,
		},
		{
			name: "selected membership removed",
			mutate: func(
				t *testing.T,
				ctx context.Context,
				db *control.DB,
				main control.Index,
				_ control.Index,
				authority *HECAdmissionAuthority,
				_ time.Time,
			) {
				t.Helper()
				if _, err := db.SQLDB().ExecContext(ctx, `
					DELETE FROM ingestion_token_indexes
					WHERE ingestion_token_id = ? AND index_id = ?`,
					authority.TokenID,
					main.ID,
				); err != nil {
					t.Fatalf("remove selected HEC token membership: %v", err)
				}
			},
			want: ErrStaleHECAdmission,
		},
		{
			name: "selected index generation changed",
			mutate: func(
				t *testing.T,
				ctx context.Context,
				db *control.DB,
				main control.Index,
				_ control.Index,
				_ *HECAdmissionAuthority,
				_ time.Time,
			) {
				t.Helper()
				definition := main.Definition
				definition.DisplayName = "changed independently"
				if _, err := db.UpdateIndex(
					ctx,
					main.ID,
					main.Version,
					definition,
				); err != nil {
					t.Fatalf("update selected index generation: %v", err)
				}
			},
			want: ErrStaleHECAdmission,
		},
		{
			name: "selected index ingestion disabled",
			mutate: func(
				t *testing.T,
				ctx context.Context,
				db *control.DB,
				_ control.Index,
				audit control.Index,
				_ *HECAdmissionAuthority,
				_ time.Time,
			) {
				t.Helper()
				definition := audit.Definition
				definition.IngestionEnabled = false
				if _, err := db.UpdateIndex(
					ctx,
					audit.ID,
					audit.Version,
					definition,
				); err != nil {
					t.Fatalf("disable selected index ingestion: %v", err)
				}
			},
			want: ErrStaleHECAdmission,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			db := openControlDB(t)
			main, err := db.CreateIndex(ctx, activeIndex("main"))
			if err != nil {
				t.Fatalf("CreateIndex(main): %v", err)
			}
			audit, err := db.CreateIndex(ctx, activeIndex("audit"))
			if err != nil {
				t.Fatalf("CreateIndex(audit): %v", err)
			}
			store, err := NewStore(
				db,
				[]byte("0123456789abcdef0123456789abcdef"),
			)
			if err != nil {
				t.Fatalf("NewStore(): %v", err)
			}
			now := time.Date(2026, 8, 10, 17, 0, 0, 0, time.UTC)
			store.now = func() time.Time { return now }
			issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
				Name:              "HEC stage authority",
				Purpose:           IngestionTokenPurposeHEC,
				AllowedIndexNames: []string{"main", "audit"},
				HECProfile: HECTokenProfile{
					IndexerAcknowledgment: true,
				},
			})
			if err != nil {
				t.Fatalf("CreateCollectorToken(HEC): %v", err)
			}
			authentication, err := store.AuthenticateHEC(
				ctx,
				issued.Secret.Plaintext(),
			)
			if err != nil {
				t.Fatalf("AuthenticateHEC(): %v", err)
			}
			authority := HECAdmissionAuthority{
				TokenID:               authentication.TokenID,
				TokenVersion:          authentication.TokenVersion,
				IndexerAcknowledgment: authentication.HECProfile.IndexerAcknowledgment,
				Indexes:               make([]HECIndexAuthoritySnapshot, 0, len(authentication.AuthorizedIndexes)),
			}
			for _, policy := range authentication.AuthorizedIndexes {
				authority.Indexes = append(authority.Indexes, HECIndexAuthoritySnapshot{
					Name:    policy.Name,
					Version: policy.Version,
				})
			}
			checkedAt := now.Add(time.Minute)
			if test.mutate != nil {
				test.mutate(t, ctx, db, main, audit, &authority, checkedAt)
			}

			tx, err := db.SQLDB().BeginTx(
				ctx,
				&sql.TxOptions{Isolation: sql.LevelSerializable},
			)
			if err != nil {
				t.Fatalf("begin HEC admission transaction: %v", err)
			}
			gotErr := RevalidateHECAdmissionInTransaction(
				ctx,
				tx,
				authority,
				checkedAt,
			)
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				t.Fatalf("rollback HEC admission transaction: %v", rollbackErr)
			}
			if !errors.Is(gotErr, test.want) {
				t.Fatalf(
					"RevalidateHECAdmissionInTransaction() error = %v, want %v",
					gotErr,
					test.want,
				)
			}
		})
	}
}

func TestRevalidateHECAdmissionInTransactionRejectsMalformedSnapshots(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	tx, err := db.SQLDB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin HEC admission transaction: %v", err)
	}
	defer tx.Rollback()
	valid := HECAdmissionAuthority{
		TokenID:      "tok_valid",
		TokenVersion: 1,
		Indexes: []HECIndexAuthoritySnapshot{{
			Name:    "main",
			Version: 1,
		}},
	}
	for label, mutate := range map[string]func(*HECAdmissionAuthority){
		"zero token version": func(value *HECAdmissionAuthority) {
			value.TokenVersion = 0
		},
		"no selected indexes": func(value *HECAdmissionAuthority) {
			value.Indexes = nil
		},
		"noncanonical index": func(value *HECAdmissionAuthority) {
			value.Indexes[0].Name = " MAIN "
		},
		"duplicate index": func(value *HECAdmissionAuthority) {
			value.Indexes = append(value.Indexes, value.Indexes[0])
		},
	} {
		authority := valid
		authority.Indexes = append([]HECIndexAuthoritySnapshot(nil), valid.Indexes...)
		mutate(&authority)
		if err := RevalidateHECAdmissionInTransaction(
			ctx,
			tx,
			authority,
			time.Now().UTC(),
		); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("%s error = %v, want ErrInvalidArgument", label, err)
		}
	}
}
