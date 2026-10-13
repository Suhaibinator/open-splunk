package common_persistence

import "errors"

// ErrSecurityTokenTransactionRequired prevents single-use account credentials
// from being consumed outside the transaction that changes the account.
var ErrSecurityTokenTransactionRequired = errors.New("security token consumption requires an ambient transaction")
