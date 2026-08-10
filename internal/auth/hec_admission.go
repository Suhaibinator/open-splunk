package auth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

// ErrStaleHECAdmission means a previously authenticated HEC snapshot no
// longer has the exact active token, profile mode, index membership, and index
// generations required for durable staging. It deliberately does not reveal
// which mutable authority changed.
var ErrStaleHECAdmission = errors.New("auth: HEC admission snapshot is stale")

// HECIndexAuthoritySnapshot identifies one selected index generation from a
// prior AuthenticateHEC result. Name is canonical and Version is the exact
// index-policy generation used to normalize and authorize the request.
type HECIndexAuthoritySnapshot struct {
	Name    string
	Version uint64
}

// HECAdmissionAuthority contains the safe, credential-free control-plane
// identities which must still be exact when a HEC request crosses durable
// staging. Indexes contains only indexes selected by at least one admitted
// event, not every index authorized for the token.
type HECAdmissionAuthority struct {
	TokenID               string
	TokenVersion          uint64
	IndexerAcknowledgment bool
	Indexes               []HECIndexAuthoritySnapshot
}

// RevalidateHECAdmissionInTransaction proves that authority remains exact in
// a caller-owned SQLite transaction. It performs bounded, count-only reads and
// neither commits nor rolls back tx. Callers must execute quota, outbox, and
// acknowledgment writes in this same transaction after it succeeds.
func RevalidateHECAdmissionInTransaction(
	ctx context.Context,
	tx *sql.Tx,
	authority HECAdmissionAuthority,
	checkedAt time.Time,
) error {
	checkedAt = databaseTime(checkedAt)
	if ctx == nil {
		return fmt.Errorf("%w: nil context", control.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if tx == nil {
		return fmt.Errorf(
			"%w: active HEC admission transaction is required",
			control.ErrInvalidArgument,
		)
	}
	if !validAuthenticationTokenID(authority.TokenID) ||
		strings.TrimSpace(authority.TokenID) != authority.TokenID {
		return fmt.Errorf(
			"%w: HEC admission token ID is invalid",
			control.ErrInvalidArgument,
		)
	}
	if authority.TokenVersion == 0 || authority.TokenVersion > math.MaxInt64 {
		return fmt.Errorf(
			"%w: HEC admission token version is invalid",
			control.ErrInvalidArgument,
		)
	}
	if checkedAt.IsZero() || checkedAt.UnixMicro() < 0 {
		return fmt.Errorf(
			"%w: HEC admission check time is invalid",
			control.ErrInvalidArgument,
		)
	}
	if len(authority.Indexes) == 0 || len(authority.Indexes) > maximumTokenScopes {
		return fmt.Errorf(
			"%w: HEC admission requires between 1 and %d selected indexes",
			control.ErrInvalidArgument,
			maximumTokenScopes,
		)
	}
	seen := make(map[string]struct{}, len(authority.Indexes))
	for _, index := range authority.Indexes {
		canonical, err := control.NormalizeIndexName(index.Name)
		if err != nil || canonical != index.Name ||
			index.Version == 0 || index.Version > math.MaxInt64 {
			return fmt.Errorf(
				"%w: HEC admission index snapshot is invalid",
				control.ErrInvalidArgument,
			)
		}
		if _, duplicate := seen[index.Name]; duplicate {
			return fmt.Errorf(
				"%w: HEC admission index snapshots must be unique",
				control.ErrInvalidArgument,
			)
		}
		seen[index.Name] = struct{}{}
	}

	acknowledgmentMode := 0
	if authority.IndexerAcknowledgment {
		acknowledgmentMode = 1
	}
	var matchingToken int64
	if err := tx.QueryRowContext(ctx, `
		SELECT count(*)
		FROM ingestion_tokens AS token
		JOIN ingestion_token_hec_profiles AS profile
		  ON profile.ingestion_token_id = token.ingestion_token_id
		WHERE token.ingestion_token_id = ?
		  AND token.version = ?
		  AND token.purpose = 'hec'
		  AND token.bound_collector_id IS NULL
		  AND token.state = 'active'
		  AND (token.expires_at_unix_micro IS NULL
		       OR token.expires_at_unix_micro > ?)
		  AND profile.indexer_acknowledgment = ?`,
		authority.TokenID,
		authority.TokenVersion,
		checkedAt.UnixMicro(),
		acknowledgmentMode,
	).Scan(&matchingToken); err != nil {
		return fmt.Errorf("revalidate HEC token admission snapshot: %w", err)
	}
	if matchingToken != 1 {
		return ErrStaleHECAdmission
	}

	var statement strings.Builder
	statement.Grow(512 + len(authority.Indexes)*8)
	statement.WriteString(`
		WITH selected(name, version) AS (VALUES `)
	arguments := make([]any, 0, len(authority.Indexes)*2+1)
	for position, index := range authority.Indexes {
		if position > 0 {
			statement.WriteByte(',')
		}
		statement.WriteString("(?, ?)")
		arguments = append(arguments, index.Name, index.Version)
	}
	statement.WriteString(`)
		SELECT count(*)
		FROM selected
		JOIN indexes AS target
		  ON target.name = selected.name
		 AND target.version = selected.version
		 AND target.state = 'active'
		 AND target.ingestion_enabled = 1
		JOIN ingestion_token_indexes AS scope
		  ON scope.index_id = target.index_id
		 AND scope.ingestion_token_id = ?`)
	arguments = append(arguments, authority.TokenID)

	var matchingIndexes int64
	if err := tx.QueryRowContext(
		ctx,
		statement.String(),
		arguments...,
	).Scan(&matchingIndexes); err != nil {
		return fmt.Errorf("revalidate HEC index admission snapshots: %w", err)
	}
	if matchingIndexes != int64(len(authority.Indexes)) {
		return ErrStaleHECAdmission
	}
	return nil
}
