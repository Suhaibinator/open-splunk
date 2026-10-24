-- Objects for postgres_break_concurrency_retry_test.go, layered on the shared
-- probe fixture baseline.

-- Classic write-skew: each writer may go off call only while another doctor
-- stays on call. Only SERIALIZABLE (with retry) preserves "someone on call".
CREATE TABLE break_on_call (
	doctor text PRIMARY KEY,
	on_call boolean NOT NULL
);

INSERT INTO break_on_call (doctor, on_call) VALUES ('alice', true), ('bob', true);

-- Two rows that writers lock in opposite orders to provoke a real deadlock.
CREATE TABLE break_accounts (
	id bigint PRIMARY KEY,
	balance bigint NOT NULL
);

INSERT INTO break_accounts (id, balance) VALUES (1, 100), (2, 100);

-- An identity column, whose sequence is not transactional, so a retried
-- insert commits a later value than its first attempt drew.
CREATE TABLE break_ledger (
	id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	note text NOT NULL
);

-- A deferred constraint trigger runs inside COMMIT. Transaction-local
-- settings choose whether it sleeps and which SQLSTATE it raises, so a test
-- can make COMMIT itself fail or stall for one attempt only.
CREATE FUNCTION break_commit_gate() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
	nap text := current_setting('break.commit_sleep', true);
	code text := current_setting('break.commit_sqlstate', true);
BEGIN
	IF nap IS NOT NULL AND nap <> '' THEN
		PERFORM pg_sleep(nap::double precision);
	END IF;
	IF code IS NOT NULL AND code <> '' THEN
		RAISE EXCEPTION 'forced commit-time failure' USING ERRCODE = code;
	END IF;
	RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER break_ledger_commit_gate
	AFTER INSERT ON break_ledger
	DEFERRABLE INITIALLY DEFERRED
	FOR EACH ROW
	EXECUTE FUNCTION break_commit_gate();
