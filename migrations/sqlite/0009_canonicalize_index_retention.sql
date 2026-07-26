-- ClickHouse stores index_time and expires_at at millisecond precision. Older
-- binaries accepted nanosecond retention values and the native writer silently
-- truncated the resulting timestamp. Preserve that effective duration for
-- existing indexes while establishing a whole-millisecond invariant:
--
-- - positive sub-millisecond values become the smallest positive stored value;
-- - larger unaligned values are floored exactly as the old native encoder did;
-- - zero remains the deployment-default sentinel.

UPDATE indexes
SET retention_nanoseconds = CASE
    WHEN retention_nanoseconds BETWEEN 1 AND 999999 THEN 1000000
    ELSE retention_nanoseconds - (retention_nanoseconds % 1000000)
END
WHERE retention_nanoseconds % 1000000 <> 0;

CREATE TRIGGER indexes_retention_is_millisecond_aligned_on_insert
BEFORE INSERT ON indexes
WHEN NEW.retention_nanoseconds % 1000000 <> 0
BEGIN
    SELECT RAISE(ABORT, 'index retention must use whole milliseconds');
END;

CREATE TRIGGER indexes_retention_is_millisecond_aligned_on_update
BEFORE UPDATE OF retention_nanoseconds ON indexes
WHEN NEW.retention_nanoseconds % 1000000 <> 0
BEGIN
    SELECT RAISE(ABORT, 'index retention must use whole milliseconds');
END;
