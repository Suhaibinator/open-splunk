package pgmeta_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgmeta"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"gorm.io/gorm"
)

func TestTransactionLockedReplaysOnlyRolledBackDeadlockAttempts(t *testing.T) {
	database, err := control.Open(t.Context(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err = database.GORMDB().Exec("CREATE TABLE retry_balance(id integer PRIMARY KEY,value integer NOT NULL);INSERT INTO retry_balance VALUES(1,0),(2,0)").Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var ready sync.WaitGroup
	ready.Add(2)
	var attempts [2]atomic.Int32
	results := make(chan error, 2)
	for worker := range 2 {
		go func() {
			results <- pgmeta.TransactionLocked(database.GORMDB().WithContext(ctx), []string{"worker-a", "worker-b"}[worker], func(tx *gorm.DB) error {
				count := attempts[worker].Add(1)
				if err := tx.Exec("SET LOCAL deadlock_timeout='100ms'").Error; err != nil {
					return err
				}
				if err := tx.Exec("UPDATE retry_balance SET value=value+1 WHERE id=?", worker+1).Error; err != nil {
					return err
				}
				if count == 1 {
					ready.Done()
					ready.Wait()
				}
				return tx.Exec("UPDATE retry_balance SET value=value+1 WHERE id=?", 2-worker).Error
			})
		}()
	}
	for range 2 {
		if err = <-results; err != nil {
			t.Fatal(err)
		}
	}
	if attempts[0].Load()+attempts[1].Load() < 3 {
		t.Fatal("fixture did not force a rolled-back deadlock")
	}
	var rows []struct{ ID, Value int }
	if err = database.GORMDB().Raw("SELECT id,value FROM retry_balance ORDER BY id").Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Value != 2 || rows[1].Value != 2 {
		t.Fatalf("partial failed attempt escaped rollback: %+v", rows)
	}
}
