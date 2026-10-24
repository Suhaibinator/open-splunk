-- open-splunk PostgreSQL control plane: draw durable_search_jobs.row_cursor
-- in commit order.
--
-- row_cursor replaces SQLite's rowid as the artifact listing's high-water
-- mark (internal/searchartifacts/list.go). A listing reads MAX(row_cursor)
-- once and then pages over row_cursor <= that mark, so the set it pages
-- stays frozen. That holds only if no row with a cursor at or below a
-- visible mark can commit later. An identity value is drawn when the INSERT
-- runs, not when it commits. Without this file, a transaction that inserted
-- first (cursor 1) but committed after another (cursor 2) appeared inside a
-- mark of 2 that a listing had already handed out. SQLite's single writer
-- could not produce that order.
--
-- A statement-level BEFORE INSERT trigger fires before the INSERT evaluates
-- any column default. Taking a transaction-scoped advisory lock there means
-- every inserting transaction draws its identity values while it holds the
-- lock, and keeps the lock until it commits or rolls back. PostgreSQL
-- releases a transaction's locks only after its commit is visible. So a
-- transaction that drew cursor k had ended before any higher cursor was
-- drawn, and every snapshot that sees cursor m also sees every committed
-- cursor below m. The identity sequence must keep its default CACHE 1: a
-- per-session cache would hand out values drawn before the lock was taken.
--
-- The cost: transactions that insert search jobs run one at a time, from
-- their first job INSERT until they end. Listing, updates and deletes do not
-- take the lock. The search artifact store already admits jobs one at a
-- time. If two job inserters also wait on each other's row locks,
-- PostgreSQL reports a deadlock (40P01), which WriteTx retries.
--
-- The key 0x6f73_0000_0000_0003 continues internal/control's "os" advisory
-- keys (0001 is the migration runner, 0002 the server singleton). A session
-- under session_replication_role = replica skips this trigger, like every
-- other trigger.

CREATE FUNCTION durable_search_job_cursor_is_commit_ordered()
RETURNS trigger
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(x'6f73000000000003'::bigint);
    RETURN NULL;
END
$$;

CREATE TRIGGER durable_search_job_cursor_is_commit_ordered
BEFORE INSERT ON durable_search_jobs
FOR EACH STATEMENT
EXECUTE FUNCTION durable_search_job_cursor_is_commit_ordered();
