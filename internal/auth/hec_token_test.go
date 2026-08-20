package auth

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/ingestquota"
)

func TestAuthenticateHECReturnsVersionedPolicySnapshotAndRecordsUse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	indexDefinition := activeIndex("main")
	indexDefinition.IngestionRateLimits = ingestquota.Limits{
		MaxEventsPerSecond:            17,
		MaxUncompressedBytesPerSecond: 4096,
	}
	createdIndex, err := db.CreateIndex(ctx, indexDefinition)
	if err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	now := time.Date(2026, 8, 10, 14, 0, 0, 123_456_789, time.UTC)
	store.now = func() time.Time { return now }
	profile := HECTokenProfile{
		DefaultIndexName:      "main",
		DefaultHost:           "producer-1",
		DefaultSource:         "/var/log/app.json",
		DefaultSourcetype:     "_json",
		IndexerAcknowledgment: true,
	}
	tokenLimits := ingestquota.Limits{
		MaxEventsPerSecond:            101,
		MaxUncompressedBytesPerSecond: 1 << 20,
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:                 "HEC authenticated",
		Purpose:              IngestionTokenPurposeHEC,
		HECProfile:           profile,
		AllowedIndexNames:    []string{"main"},
		AllowedHostRegexes:   []string{"^producer-[0-9]+$"},
		AllowedSourceRegexes: []string{"^/var/log/.+$"},
		IngestionRateLimits:  tokenLimits,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(HEC): %v", err)
	}
	native, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "native isolation",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(native): %v", err)
	}

	authentication, err := store.AuthenticateHEC(ctx, issued.Secret.Plaintext())
	if err != nil {
		t.Fatalf("AuthenticateHEC(): %v", err)
	}
	if authentication.TokenID != issued.Token.ID ||
		authentication.TokenVersion != issued.Token.Version ||
		authentication.TokenName != issued.Token.Name ||
		authentication.Purpose != IngestionTokenPurposeHEC ||
		authentication.BoundCollectorID != "" ||
		authentication.HECProfile != profile ||
		authentication.TokenRateLimits != tokenLimits ||
		!slices.Equal(authentication.AllowedHostRegexes, []string{"^producer-[0-9]+$"}) ||
		!slices.Equal(authentication.AllowedSourceRegexes, []string{"^/var/log/.+$"}) ||
		!slices.Equal(authentication.AuthorizedIndexNames(), []string{"main"}) {
		t.Fatalf("AuthenticateHEC() snapshot = %#v", authentication)
	}
	if len(authentication.AuthorizedIndexes) != 1 ||
		authentication.AuthorizedIndexes[0].Version != createdIndex.Version ||
		authentication.AuthorizedIndexes[0].IngestionRateLimits !=
			indexDefinition.IngestionRateLimits {
		t.Fatalf(
			"AuthenticateHEC() index policies = %#v",
			authentication.AuthorizedIndexes,
		)
	}
	persisted, err := store.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(HEC after authentication): %v", err)
	}
	if !persisted.LastUsedAt.Equal(databaseTime(now)) ||
		persisted.Version != issued.Token.Version ||
		!persisted.UpdatedAt.Equal(issued.Token.UpdatedAt) {
		t.Fatalf("HEC last-use projection = %#v", persisted)
	}
	if _, err := store.AuthenticateHEC(
		ctx,
		native.Secret.Plaintext(),
	); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("AuthenticateHEC(native) error = %v, want ErrUnauthorized", err)
	}
	nativeMetadata, err := store.GetCollectorToken(ctx, native.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(native): %v", err)
	}
	if !nativeMetadata.LastUsedAt.IsZero() {
		t.Fatalf("wrong-purpose HEC authentication recorded native use at %v", nativeMetadata.LastUsedAt)
	}
}

func TestAuthenticateHECFailsClosedWithoutProfileAndDoesNotRecordUse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	now := time.Date(2026, 8, 10, 15, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "HEC corrupt profile",
		Purpose:           IngestionTokenPurposeHEC,
		AllowedIndexNames: []string{"main"},
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(HEC): %v", err)
	}
	if _, err := db.SQLDB().ExecContext(
		ctx,
		`DELETE FROM ingestion_token_hec_profiles WHERE ingestion_token_id = ?`,
		issued.Token.ID,
	); err != nil {
		t.Fatalf("remove HEC profile for corruption test: %v", err)
	}

	if _, err := store.AuthenticateHEC(ctx, issued.Secret.Plaintext()); err == nil ||
		errors.Is(err, ErrUnauthorized) {
		t.Fatalf("AuthenticateHEC(missing profile) error = %v, want fail-closed integrity error", err)
	}
	var lastUsedAt *int64
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT last_used_at_unix_micro
		FROM ingestion_tokens
		WHERE ingestion_token_id = ?`, issued.Token.ID).Scan(&lastUsedAt); err != nil {
		t.Fatalf("read HEC last-use after corrupt authentication: %v", err)
	}
	if lastUsedAt != nil {
		t.Fatalf("corrupt HEC authentication recorded last use %d", *lastUsedAt)
	}
}

func TestAuthenticateHECRejectsInactiveExpiredRevokedAndUnknownCredentials(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	now := time.Date(2026, 8, 10, 16, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	create := func(name string, expiresAt time.Time) IssuedCollectorToken {
		t.Helper()
		issued, createErr := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
			Name:              name,
			Purpose:           IngestionTokenPurposeHEC,
			AllowedIndexNames: []string{"main"},
			ExpiresAt:         expiresAt,
		})
		if createErr != nil {
			t.Fatalf("CreateCollectorToken(%s): %v", name, createErr)
		}
		return issued
	}
	disabled := create("HEC disabled", time.Time{})
	if _, err := db.SQLDB().ExecContext(ctx, `
		UPDATE ingestion_tokens
		SET state = 'disabled'
		WHERE ingestion_token_id = ?`, disabled.Token.ID); err != nil {
		t.Fatalf("disable HEC token: %v", err)
	}
	expired := create("HEC expired", now.Add(time.Minute))
	revoked := create("HEC revoked", time.Time{})
	if _, err := store.RevokeCollectorToken(
		ctx,
		revoked.Token.ID,
		revoked.Token.Version,
	); err != nil {
		t.Fatalf("RevokeCollectorToken(HEC): %v", err)
	}
	store.now = func() time.Time { return now.Add(2 * time.Minute) }

	for label, test := range map[string]struct {
		credential string
		want       error
	}{
		"disabled": {disabled.Secret.Plaintext(), ErrInactiveToken},
		"expired":  {expired.Secret.Plaintext(), ErrUnauthorized},
		"revoked":  {revoked.Secret.Plaintext(), ErrUnauthorized},
		"unknown":  {"ost_not-a-real-credential", ErrUnauthorized},
	} {
		if _, err := store.AuthenticateHEC(
			ctx,
			test.credential,
		); !errors.Is(err, test.want) {
			t.Fatalf("AuthenticateHEC(%s) error = %v, want %v", label, err, test.want)
		}
	}
	for _, issued := range []IssuedCollectorToken{disabled, expired, revoked} {
		var lastUsedAt *int64
		if err := db.SQLDB().QueryRowContext(ctx, `
			SELECT last_used_at_unix_micro
			FROM ingestion_tokens
			WHERE ingestion_token_id = ?`, issued.Token.ID).Scan(&lastUsedAt); err != nil {
			t.Fatalf("read HEC last-use after rejected authentication: %v", err)
		}
		if lastUsedAt != nil {
			t.Fatalf("rejected HEC authentication recorded last use %d", *lastUsedAt)
		}
	}
}

func TestHECTokenLifecycleAndNativeAuthorizationIsolation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	mainIndex, err := db.CreateIndex(ctx, activeIndex("main"))
	if err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	if _, err := db.CreateIndex(ctx, activeIndex("audit")); err != nil {
		t.Fatalf("CreateIndex(audit): %v", err)
	}
	store, err := NewStore(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:                 "HEC production",
		Description:          "HTTP event ingestion",
		Purpose:              IngestionTokenPurposeHEC,
		AllowedIndexNames:    []string{"main", "audit"},
		AllowedHostRegexes:   []string{"hec-[a-z]+"},
		AllowedSourceRegexes: []string{"/var/log/.+"},
		HECProfile: HECTokenProfile{
			DefaultIndexName:      "main",
			DefaultHost:           "hec-producer",
			DefaultSource:         "/var/log/app.json",
			DefaultSourcetype:     "_json",
			IndexerAcknowledgment: true,
		},
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(HEC): %v", err)
	}
	if issued.Token.Purpose != IngestionTokenPurposeHEC ||
		issued.Token.BoundCollectorID != "" ||
		issued.Token.HECProfile.DefaultIndexName != "main" ||
		!issued.Token.HECProfile.IndexerAcknowledgment {
		t.Fatalf("issued HEC metadata = %#v", issued.Token)
	}

	var storedPurpose, defaultIndexID, defaultHost string
	var indexerAcknowledgment int
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT token.purpose, profile.default_index_id,
		       profile.default_host, profile.indexer_acknowledgment
		FROM ingestion_tokens AS token
		JOIN ingestion_token_hec_profiles AS profile
		  ON profile.ingestion_token_id = token.ingestion_token_id
		WHERE token.ingestion_token_id = ?`, issued.Token.ID).
		Scan(
			&storedPurpose,
			&defaultIndexID,
			&defaultHost,
			&indexerAcknowledgment,
		); err != nil {
		t.Fatalf("read stored HEC profile: %v", err)
	}
	if storedPurpose != string(IngestionTokenPurposeHEC) ||
		defaultIndexID != mainIndex.ID ||
		defaultHost != "hec-producer" ||
		indexerAcknowledgment != 1 {
		t.Fatalf(
			"stored HEC profile = purpose:%q index:%q host:%q ack:%d",
			storedPurpose,
			defaultIndexID,
			defaultHost,
			indexerAcknowledgment,
		)
	}

	plaintext := issued.Secret.Plaintext()
	if _, err := store.Authenticate(ctx, plaintext); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Authenticate(HEC on native boundary) error = %v, want ErrUnauthorized", err)
	}
	if _, err := store.Authorize(ctx, plaintext, "main"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("Authorize(HEC on native boundary) error = %v, want ErrUnauthorized", err)
	}
	if err := store.RecordCollectorTokenUse(
		ctx,
		issued.Token.ID,
		now.Add(time.Minute),
	); !errors.Is(err, ErrInactiveToken) {
		t.Fatalf("RecordCollectorTokenUse(HEC) error = %v, want ErrInactiveToken", err)
	}

	got, err := store.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(HEC): %v", err)
	}
	if got.Purpose != issued.Token.Purpose || got.HECProfile != issued.Token.HECProfile {
		t.Fatalf("HEC get metadata = %#v, want %#v", got, issued.Token)
	}
	listed, err := store.ListCollectorTokens(ctx)
	if err != nil {
		t.Fatalf("ListCollectorTokens(): %v", err)
	}
	if len(listed) != 1 || listed[0].Purpose != IngestionTokenPurposeHEC ||
		listed[0].HECProfile != issued.Token.HECProfile {
		t.Fatalf("listed HEC tokens = %#v", listed)
	}

	updatedProfile := got.HECProfile
	updatedProfile.DefaultIndexName = "audit"
	updatedProfile.DefaultHost = "replacement-producer"
	updatedProfile.DefaultSource = ""
	updated, err := store.UpdateCollectorToken(
		ctx,
		got.ID,
		got.Version,
		UpdateCollectorTokenRequest{
			Name:                 "HEC production updated",
			Description:          got.Description,
			Purpose:              IngestionTokenPurposeHEC,
			HECProfile:           updatedProfile,
			AllowedIndexNames:    got.AllowedIndexNames,
			AllowedHostRegexes:   got.AllowedHostRegexes,
			AllowedSourceRegexes: got.AllowedSourceRegexes,
		},
	)
	if err != nil {
		t.Fatalf("UpdateCollectorToken(HEC defaults): %v", err)
	}
	if updated.Version != got.Version+1 ||
		updated.Purpose != IngestionTokenPurposeHEC ||
		updated.HECProfile != updatedProfile {
		t.Fatalf("updated HEC metadata = %#v", updated)
	}

	for label, mutation := range map[string]UpdateCollectorTokenRequest{
		"purpose": {
			Name:              updated.Name,
			Purpose:           IngestionTokenPurposeNativeCollector,
			BoundCollectorID:  testCollectorID,
			AllowedIndexNames: updated.AllowedIndexNames,
		},
		"acknowledgment": {
			Name:    updated.Name,
			Purpose: IngestionTokenPurposeHEC,
			HECProfile: HECTokenProfile{
				DefaultIndexName:      updated.HECProfile.DefaultIndexName,
				DefaultHost:           updated.HECProfile.DefaultHost,
				DefaultSourcetype:     updated.HECProfile.DefaultSourcetype,
				IndexerAcknowledgment: false,
			},
			AllowedIndexNames: updated.AllowedIndexNames,
		},
		"collector binding": {
			Name:              updated.Name,
			Purpose:           IngestionTokenPurposeHEC,
			HECProfile:        updated.HECProfile,
			BoundCollectorID:  testCollectorID,
			AllowedIndexNames: updated.AllowedIndexNames,
		},
		"default outside scope": {
			Name:    updated.Name,
			Purpose: IngestionTokenPurposeHEC,
			HECProfile: HECTokenProfile{
				DefaultIndexName:      "main",
				IndexerAcknowledgment: true,
			},
			AllowedIndexNames: []string{"audit"},
		},
	} {
		if _, err := store.UpdateCollectorToken(
			ctx,
			updated.ID,
			updated.Version,
			mutation,
		); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("UpdateCollectorToken(%s) error = %v, want ErrInvalidArgument", label, err)
		}
	}
	unchanged, err := store.GetCollectorToken(ctx, updated.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(after rejected updates): %v", err)
	}
	if unchanged.Version != updated.Version || unchanged.HECProfile != updated.HECProfile {
		t.Fatalf("rejected HEC update mutated token: %#v", unchanged)
	}

	for label, statement := range map[string]string{
		"purpose":              `UPDATE ingestion_tokens SET purpose = 'native_collector' WHERE ingestion_token_id = ?`,
		"acknowledgment":       `UPDATE ingestion_token_hec_profiles SET indexer_acknowledgment = 0 WHERE ingestion_token_id = ?`,
		"out-of-scope default": `UPDATE ingestion_token_hec_profiles SET default_index_id = 'missing-index' WHERE ingestion_token_id = ?`,
	} {
		if _, err := db.SQLDB().ExecContext(ctx, statement, updated.ID); err == nil {
			t.Fatalf("direct HEC %s mutation unexpectedly succeeded", label)
		}
	}

	revoked, err := store.RevokeCollectorToken(ctx, updated.ID, updated.Version)
	if err != nil {
		t.Fatalf("RevokeCollectorToken(HEC): %v", err)
	}
	if revoked.State != CollectorTokenStateRevoked ||
		revoked.Purpose != IngestionTokenPurposeHEC ||
		revoked.HECProfile != updated.HECProfile {
		t.Fatalf("revoked HEC token = %#v", revoked)
	}
	if _, err := db.SQLDB().ExecContext(
		ctx,
		`DELETE FROM ingestion_tokens WHERE ingestion_token_id = ?`,
		revoked.ID,
	); err != nil {
		t.Fatalf("delete revoked HEC tombstone with cascaded profile: %v", err)
	}
}

func TestIngestionTokenPurposeCreationValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}

	native, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "legacy native default",
		AllowedIndexNames: []string{"main"},
		BoundCollectorID:  testCollectorID,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(legacy native default): %v", err)
	}
	if native.Token.Purpose != IngestionTokenPurposeNativeCollector ||
		native.Token.HECProfile != (HECTokenProfile{}) {
		t.Fatalf("native compatibility purpose = %#v", native.Token)
	}
	if _, err := store.Authenticate(ctx, native.Secret.Plaintext()); err != nil {
		t.Fatalf("Authenticate(native): %v", err)
	}

	for label, request := range map[string]CreateCollectorTokenRequest{
		"unknown purpose": {
			Name:              "unknown",
			Purpose:           IngestionTokenPurpose("unknown"),
			AllowedIndexNames: []string{"main"},
		},
		"native HEC profile": {
			Name:              "native profile",
			Purpose:           IngestionTokenPurposeNativeCollector,
			BoundCollectorID:  "collector-native-profile",
			AllowedIndexNames: []string{"main"},
			HECProfile:        HECTokenProfile{DefaultHost: "forbidden"},
		},
		"HEC collector binding": {
			Name:              "HEC binding",
			Purpose:           IngestionTokenPurposeHEC,
			BoundCollectorID:  "collector-hec-forbidden",
			AllowedIndexNames: []string{"main"},
		},
		"HEC noncanonical default index": {
			Name:              "HEC bad index",
			Purpose:           IngestionTokenPurposeHEC,
			AllowedIndexNames: []string{"main"},
			HECProfile:        HECTokenProfile{DefaultIndexName: " MAIN "},
		},
		"HEC default index outside scope": {
			Name:              "HEC outside index",
			Purpose:           IngestionTokenPurposeHEC,
			AllowedIndexNames: []string{"main"},
			HECProfile:        HECTokenProfile{DefaultIndexName: "audit"},
		},
		"HEC whitespace metadata": {
			Name:              "HEC whitespace",
			Purpose:           IngestionTokenPurposeHEC,
			AllowedIndexNames: []string{"main"},
			HECProfile:        HECTokenProfile{DefaultHost: " producer "},
		},
	} {
		if _, err := store.CreateCollectorToken(
			ctx,
			request,
		); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("CreateCollectorToken(%s) error = %v, want ErrInvalidArgument", label, err)
		}
	}
}

func TestHECMetadataDefaultPreservesNonASCIIEdgeWhitespace(t *testing.T) {
	t.Parallel()
	if !validHECMetadataDefault("\u00a0producer\u00a0") {
		t.Fatal("non-ASCII edge whitespace must be preserved by the HEC token contract")
	}
	for _, value := range []string{" producer", "producer ", "\tproducer", "producer\n"} {
		if validHECMetadataDefault(value) {
			t.Fatalf("ASCII edge whitespace accepted in %q", value)
		}
	}
}
