package auth

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

func TestServerKeyStateGORMModelMatchesMigratedSQLiteSchema(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	statement := &gorm.Statement{DB: database.GORMDB()}
	if err := statement.Parse(&serverKeyStateRecord{}); err != nil {
		t.Fatalf("parse GORM server-key model: %v", err)
	}

	type columnRow struct {
		Name       string `gorm:"column:name"`
		Type       string `gorm:"column:type"`
		NotNull    int64  `gorm:"column:not_null"`
		PrimaryKey int64  `gorm:"column:primary_key"`
	}
	var migratedColumns []columnRow
	query := database.GORMDB().WithContext(ctx).Raw(
		`SELECT
			name,
			type,
			"notnull" AS not_null,
			pk AS primary_key
		 FROM pragma_table_info('server_key_state')
		 ORDER BY cid`,
	).Scan(&migratedColumns)
	if query.Error != nil {
		t.Fatalf("read migrated server-key columns: %v", query.Error)
	}
	wantColumns := []string{"key_name", "fingerprint", "created_at_unix_micro"}
	if !slices.Equal(statement.Schema.DBNames, wantColumns) {
		t.Fatalf(
			"GORM server-key columns = %v, want %v",
			statement.Schema.DBNames,
			wantColumns,
		)
	}
	wantMigratedColumns := []columnRow{
		{Name: "key_name", Type: "TEXT", NotNull: 1, PrimaryKey: 1},
		{Name: "fingerprint", Type: "BLOB", NotNull: 1},
		{Name: "created_at_unix_micro", Type: "INTEGER", NotNull: 1},
	}
	if !slices.Equal(migratedColumns, wantMigratedColumns) {
		t.Fatalf(
			"migrated server-key columns = %#v, want %#v",
			migratedColumns,
			wantMigratedColumns,
		)
	}
	if len(statement.Schema.PrimaryFields) != 1 ||
		statement.Schema.PrimaryFields[0].DBName != "key_name" {
		t.Fatalf(
			"GORM server-key primary fields = %#v, want key_name",
			statement.Schema.PrimaryFields,
		)
	}
	for _, want := range wantMigratedColumns {
		field := statement.Schema.LookUpField(want.Name)
		if field == nil ||
			!strings.EqualFold(string(field.DataType), want.Type) ||
			field.NotNull != (want.NotNull == 1) ||
			field.PrimaryKey != (want.PrimaryKey == 1) {
			t.Errorf(
				"GORM server-key field %q = %#v, want type=%s not-null=%t primary=%t",
				want.Name,
				field,
				want.Type,
				want.NotNull == 1,
				want.PrimaryKey == 1,
			)
		}
	}

	wantChecks := map[string]string{
		"server_key_state_fingerprint_sha256": "length(fingerprint) = 32",
		"server_key_state_name_fixed":         "key_name = 'server-master-v1'",
	}
	checks := statement.Schema.ParseCheckConstraints()
	if len(checks) != len(wantChecks) {
		t.Fatalf("GORM server-key checks = %v, want %v", checks, wantChecks)
	}
	for name, expression := range wantChecks {
		check, exists := checks[name]
		if !exists || check.Constraint != expression {
			t.Errorf("GORM server-key check %s = %#v, want %q", name, check, expression)
		}
	}

	type definitionRow struct {
		Definition string `gorm:"column:definition"`
	}
	var definition definitionRow
	query = database.GORMDB().WithContext(ctx).Raw(
		`SELECT sql AS definition
		 FROM sqlite_schema
		 WHERE type = 'table' AND name = 'server_key_state'`,
	).Scan(&definition)
	if query.Error != nil {
		t.Fatalf("read migrated server-key definition: %v", query.Error)
	}
	normalized := strings.Join(strings.Fields(definition.Definition), " ")
	if !strings.Contains(normalized, ") STRICT") {
		t.Errorf("server_key_state is not STRICT: %s", normalized)
	}
	if !strings.Contains(
		normalized,
		"key_name TEXT PRIMARY KEY NOT NULL COLLATE BINARY",
	) {
		t.Errorf("server_key_state key collation drifted: %s", normalized)
	}
	for _, expression := range wantChecks {
		if !strings.Contains(normalized, "CHECK ("+expression+")") {
			t.Errorf(
				"migrated server-key definition does not contain check %q: %s",
				expression,
				normalized,
			)
		}
	}
}

func TestServerMasterKeyIdentityLifecycleIsDetachedAndIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if fingerprint, registered, err := ReadServerMasterKeyIdentity(ctx, database); err != nil {
		t.Fatalf("ReadServerMasterKeyIdentity(empty): %v", err)
	} else if registered || fingerprint != nil {
		t.Fatalf(
			"ReadServerMasterKeyIdentity(empty) = (%x, %t), want (nil, false)",
			fingerprint,
			registered,
		)
	}
	if err := ValidateServerMasterKeyInitialization(ctx, database); err != nil {
		t.Fatalf("ValidateServerMasterKeyInitialization(empty): %v", err)
	}

	fingerprint := bytes.Repeat([]byte{0x5a}, sha256.Size)
	registeredAt := time.Date(2026, 7, 28, 18, 30, 0, 123456000, time.UTC)
	if err := registerServerMasterKeyIdentityAt(
		ctx,
		database,
		fingerprint,
		registeredAt,
	); err != nil {
		t.Fatalf("registerServerMasterKeyIdentityAt(first): %v", err)
	}
	stored, registered, err := ReadServerMasterKeyIdentity(ctx, database)
	if err != nil {
		t.Fatalf("ReadServerMasterKeyIdentity(registered): %v", err)
	}
	if !registered || !bytes.Equal(stored, fingerprint) {
		t.Fatalf(
			"ReadServerMasterKeyIdentity(registered) = (%x, %t), want (%x, true)",
			stored,
			registered,
			fingerprint,
		)
	}
	stored[0] ^= 0xff
	reloaded, registered, err := ReadServerMasterKeyIdentity(ctx, database)
	if err != nil {
		t.Fatalf("ReadServerMasterKeyIdentity(reloaded): %v", err)
	}
	if !registered || !bytes.Equal(reloaded, fingerprint) {
		t.Fatalf("caller mutation changed persisted fingerprint: %x", reloaded)
	}

	later := registeredAt.Add(time.Hour)
	if err := registerServerMasterKeyIdentityAt(
		ctx,
		database,
		fingerprint,
		later,
	); err != nil {
		t.Fatalf("registerServerMasterKeyIdentityAt(idempotent): %v", err)
	}
	var persisted serverKeyStateRecord
	if err := database.GORMDB().WithContext(ctx).Take(&persisted).Error; err != nil {
		t.Fatalf("read persisted server-key row: %v", err)
	}
	if persisted.CreatedAtUnixMicro != registeredAt.UnixMicro() {
		t.Fatalf(
			"idempotent registration changed creation time to %d, want %d",
			persisted.CreatedAtUnixMicro,
			registeredAt.UnixMicro(),
		)
	}

	other := bytes.Repeat([]byte{0xa5}, sha256.Size)
	if err := registerServerMasterKeyIdentityAt(
		ctx,
		database,
		other,
		later,
	); !errors.Is(err, ErrServerMasterKeyIdentityConflict) {
		t.Fatalf(
			"registerServerMasterKeyIdentityAt(other) error = %v, want conflict",
			err,
		)
	}
	reloaded, _, err = ReadServerMasterKeyIdentity(ctx, database)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reloaded, fingerprint) {
		t.Fatalf("conflicting registration replaced fingerprint: %x", reloaded)
	}
}

func TestServerMasterKeyIdentityRejectsInvalidDependenciesAndCancellation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	validFingerprint := bytes.Repeat([]byte{1}, sha256.Size)
	for name, operation := range map[string]func() error{
		"nil context read": func() error {
			//nolint:staticcheck // This case explicitly verifies the nil-context guard.
			_, _, err := ReadServerMasterKeyIdentity(nil, database)
			return err
		},
		"nil database read": func() error {
			_, _, err := ReadServerMasterKeyIdentity(ctx, nil)
			return err
		},
		"nil context validation": func() error {
			//nolint:staticcheck // This case explicitly verifies the nil-context guard.
			return ValidateServerMasterKeyInitialization(nil, database)
		},
		"nil database registration": func() error {
			return RegisterServerMasterKeyIdentity(ctx, nil, validFingerprint)
		},
		"short fingerprint": func() error {
			return RegisterServerMasterKeyIdentity(ctx, database, validFingerprint[:31])
		},
		"zero registration time": func() error {
			return registerServerMasterKeyIdentityAt(
				ctx,
				database,
				validFingerprint,
				time.Time{},
			)
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := operation(); !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("operation error = %v, want control.ErrInvalidArgument", err)
			}
		})
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := ReadServerMasterKeyIdentity(
		canceled,
		database,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read error = %v, want context.Canceled", err)
	}
	if err := ValidateServerMasterKeyInitialization(
		canceled,
		database,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled validation error = %v, want context.Canceled", err)
	}
	if err := RegisterServerMasterKeyIdentity(
		canceled,
		database,
		validFingerprint,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled registration error = %v, want context.Canceled", err)
	}
}

func TestConcurrentServerMasterKeyRegistrationSelectsOneIdentity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	const contenders = 12
	start := make(chan struct{})
	type result struct {
		fingerprint []byte
		err         error
	}
	results := make(chan result, contenders)
	var workers sync.WaitGroup
	for contender := range contenders {
		workers.Add(1)
		go func() {
			defer workers.Done()
			fingerprint := bytes.Repeat([]byte{byte(contender + 1)}, sha256.Size)
			<-start
			err := registerServerMasterKeyIdentityAt(
				ctx,
				database,
				fingerprint,
				time.Date(2026, 7, 28, 19, contender, 0, 0, time.UTC),
			)
			results <- result{fingerprint: fingerprint, err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)

	var winner []byte
	conflicts := 0
	for registration := range results {
		switch {
		case registration.err == nil:
			if winner != nil {
				t.Fatalf(
					"multiple distinct fingerprints registered: %x and %x",
					winner,
					registration.fingerprint,
				)
			}
			winner = registration.fingerprint
		case errors.Is(registration.err, ErrServerMasterKeyIdentityConflict):
			conflicts++
		default:
			t.Fatalf("registration error = %v, want conflict", registration.err)
		}
	}
	if winner == nil || conflicts != contenders-1 {
		t.Fatalf(
			"winner=%x conflicts=%d, want one winner and %d conflicts",
			winner,
			conflicts,
			contenders-1,
		)
	}
	stored, registered, err := ReadServerMasterKeyIdentity(ctx, database)
	if err != nil {
		t.Fatal(err)
	}
	if !registered || !bytes.Equal(stored, winner) {
		t.Fatalf("persisted fingerprint = %x, want winner %x", stored, winner)
	}
}

func TestServerMasterKeyRegistrationRefusesExistingCollectorTokens(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	tokens, err := NewStore(
		database,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	if _, err := tokens.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "unbound master-key test",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  "server-key-test-collector",
	}); err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}

	if err := ValidateServerMasterKeyInitialization(
		ctx,
		database,
	); !errors.Is(err, ErrServerMasterKeyIdentityUnsafe) {
		t.Fatalf(
			"ValidateServerMasterKeyInitialization() error = %v, want unsafe",
			err,
		)
	}
	fingerprint := bytes.Repeat([]byte{0x77}, sha256.Size)
	if err := RegisterServerMasterKeyIdentity(
		ctx,
		database,
		fingerprint,
	); !errors.Is(err, ErrServerMasterKeyIdentityUnsafe) {
		t.Fatalf("RegisterServerMasterKeyIdentity() error = %v, want unsafe", err)
	}
	if stored, registered, err := ReadServerMasterKeyIdentity(ctx, database); err != nil {
		t.Fatal(err)
	} else if registered || stored != nil {
		t.Fatalf("unsafe registration persisted identity (%x, %t)", stored, registered)
	}
}

func TestServerMasterKeyRegistrationSerializesWithTokenCreation(t *testing.T) {
	t.Parallel()

	t.Run("token transaction commits first", func(t *testing.T) {
		ctx := context.Background()
		database := openControlDB(t)
		tx := database.GORMDB().WithContext(ctx).Begin()
		if tx.Error != nil {
			t.Fatalf("begin forced token-first transaction: %v", tx.Error)
		}
		finished := false
		t.Cleanup(func() {
			if !finished {
				_ = tx.Rollback().Error
			}
		})
		binding := "token-first-collector"
		now := time.Date(2026, 7, 28, 20, 0, 0, 0, time.UTC).UnixMicro()
		record := collectorTokenRecord{
			IngestionTokenID:   "tok_server_key_token_first",
			Version:            1,
			Name:               "token first",
			TokenPrefix:        "ost_v1_tokenfirst",
			TokenDigest:        bytes.Repeat([]byte{0x41}, sha256.Size),
			State:              CollectorTokenStateActive,
			CreatedAtUnixMicro: now,
			UpdatedAtUnixMicro: now,
			BoundCollectorID:   &binding,
		}
		if err := tx.Create(&record).Error; err != nil {
			t.Fatalf("seed uncommitted collector token: %v", err)
		}

		fingerprint := bytes.Repeat([]byte{0x33}, sha256.Size)
		started := make(chan struct{})
		registrationResult := make(chan error, 1)
		go func() {
			close(started)
			registrationResult <- RegisterServerMasterKeyIdentity(
				ctx,
				database,
				fingerprint,
			)
		}()
		<-started
		if err := tx.Commit().Error; err != nil {
			t.Fatalf("commit forced token-first transaction: %v", err)
		}
		finished = true
		if err := <-registrationResult; !errors.Is(
			err,
			ErrServerMasterKeyIdentityUnsafe,
		) {
			t.Fatalf("token-first registration error = %v, want unsafe", err)
		}
		if stored, registered, err := ReadServerMasterKeyIdentity(
			ctx,
			database,
		); err != nil {
			t.Fatal(err)
		} else if registered || stored != nil {
			t.Fatalf("token-first registration persisted identity: %x", stored)
		}
	})

	t.Run("identity transaction commits first", func(t *testing.T) {
		ctx := context.Background()
		database := openControlDB(t)
		if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
			t.Fatalf("CreateIndex(main): %v", err)
		}
		tokens, err := NewStore(
			database,
			[]byte("0123456789abcdef0123456789abcdef"),
		)
		if err != nil {
			t.Fatalf("NewStore(): %v", err)
		}

		fingerprint := bytes.Repeat([]byte{0x66}, sha256.Size)
		tx := database.GORMDB().WithContext(ctx).Begin()
		if tx.Error != nil {
			t.Fatalf("begin forced registration-first transaction: %v", tx.Error)
		}
		finished := false
		t.Cleanup(func() {
			if !finished {
				_ = tx.Rollback().Error
			}
		})
		if err := registerServerKeyStateInTransaction(
			tx,
			fingerprint,
			time.Date(2026, 7, 28, 21, 0, 0, 0, time.UTC).UnixMicro(),
		); err != nil {
			t.Fatalf("stage server-key identity: %v", err)
		}

		started := make(chan struct{})
		tokenResult := make(chan error, 1)
		go func() {
			close(started)
			_, createErr := tokens.CreateCollectorToken(
				ctx,
				CreateCollectorTokenRequest{
					Name:              "registration first",
					AllowedIndexNames: []string{"main"},
					BoundCollectorID:  "registration-first-collector",
				},
			)
			tokenResult <- createErr
		}()
		<-started
		if err := tx.Commit().Error; err != nil {
			t.Fatalf("commit forced registration-first transaction: %v", err)
		}
		finished = true
		if err := <-tokenResult; err != nil {
			t.Fatalf("registration-first token creation: %v", err)
		}
		stored, registered, err := ReadServerMasterKeyIdentity(ctx, database)
		if err != nil {
			t.Fatal(err)
		}
		if !registered || !bytes.Equal(stored, fingerprint) {
			t.Fatalf("registration-first identity = %x, want %x", stored, fingerprint)
		}
		if err := ValidateServerMasterKeyInitialization(ctx, database); err != nil {
			t.Fatalf("registered database failed initialization validation: %v", err)
		}
	})
}

func TestServerMasterKeyIdentityFailsClosedOnCorruptRows(t *testing.T) {
	t.Parallel()

	tests := [][]serverKeyStateRecord{
		{{
			KeyName:            serverMasterKeyIdentityName,
			Fingerprint:        bytes.Repeat([]byte{1}, sha256.Size-1),
			CreatedAtUnixMicro: 1,
		}},
		{{
			KeyName:            serverMasterKeyIdentityName,
			Fingerprint:        bytes.Repeat([]byte{2}, sha256.Size),
			CreatedAtUnixMicro: 0,
		}},
		{{
			KeyName:            "unexpected-key",
			Fingerprint:        bytes.Repeat([]byte{3}, sha256.Size),
			CreatedAtUnixMicro: 1,
		}},
		{
			{
				KeyName:            serverMasterKeyIdentityName,
				Fingerprint:        bytes.Repeat([]byte{4}, sha256.Size),
				CreatedAtUnixMicro: 1,
			},
			{
				KeyName:            "unexpected-key",
				Fingerprint:        bytes.Repeat([]byte{5}, sha256.Size),
				CreatedAtUnixMicro: 1,
			},
		},
	}
	for position, records := range tests {
		t.Run(fmt.Sprintf("corruption-%d", position), func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			database := openControlDB(t)
			replaceServerKeyStateWithUncheckedTable(t, database)
			if err := database.GORMDB().WithContext(ctx).Create(&records).Error; err != nil {
				t.Fatalf("seed corrupt server-key record: %v", err)
			}
			if _, _, err := ReadServerMasterKeyIdentity(
				ctx,
				database,
			); !errors.Is(err, ErrServerMasterKeyIdentityCorrupt) {
				t.Fatalf("corrupt read error = %v, want corruption", err)
			}
			if err := ValidateServerMasterKeyInitialization(
				ctx,
				database,
			); !errors.Is(err, ErrServerMasterKeyIdentityCorrupt) {
				t.Fatalf("corrupt validation error = %v, want corruption", err)
			}
			if err := RegisterServerMasterKeyIdentity(
				ctx,
				database,
				bytes.Repeat([]byte{9}, sha256.Size),
			); !errors.Is(err, ErrServerMasterKeyIdentityCorrupt) {
				t.Fatalf("corrupt registration error = %v, want corruption", err)
			}
		})
	}
}

func replaceServerKeyStateWithUncheckedTable(
	t *testing.T,
	database *control.DB,
) {
	t.Helper()
	ctx := context.Background()
	if err := database.GORMDB().WithContext(ctx).Exec(
		"DROP TABLE server_key_state",
	).Error; err != nil {
		t.Fatalf("drop checked server-key table: %v", err)
	}
	if err := database.GORMDB().WithContext(ctx).Exec(`
		CREATE TABLE server_key_state (
			key_name TEXT PRIMARY KEY NOT NULL,
			fingerprint BLOB NOT NULL,
			created_at_unix_micro INTEGER NOT NULL
		) STRICT
	`).Error; err != nil {
		t.Fatalf("create unchecked server-key table: %v", err)
	}
}
