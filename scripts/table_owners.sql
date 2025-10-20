-- filepath: /Users/garth/src/springtail2/scripts/table_owners.sql

CREATE SCHEMA IF NOT EXISTS __pg_springtail_triggers;

DROP TABLE IF EXISTS __pg_springtail_triggers.table_owner_snapshot_history;
CREATE TABLE IF NOT EXISTS __pg_springtail_triggers.table_owner_snapshot_history (
    fdw_id TEXT NOT NULL,
    table_oid OID NOT NULL,
    table_name TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    schema_oid OID NOT NULL,
    role_owner_oid OID NOT NULL,
    role_owner_name TEXT NOT NULL,
    snapshot_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (fdw_id, table_oid)
);

DROP FUNCTION IF EXISTS __pg_springtail_triggers.reset_table_owner_diff;
/**
 * This function resets the table owner snapshot history for a given fdw_id.
 * If fdw_id_var is NULL, it truncates the entire history table.
 * If fdw_id_var is provided, it deletes only the entries for that specific fdw_id.
 */
CREATE OR REPLACE FUNCTION __pg_springtail_triggers.reset_table_owner_diff(fdw_id_var TEXT)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO __pg_springtail_triggers
AS $$
BEGIN
    IF fdw_id_var IS NULL THEN
        TRUNCATE __pg_springtail_triggers.table_owner_snapshot_history;
    ELSE
        DELETE FROM __pg_springtail_triggers.table_owner_snapshot_history
            WHERE fdw_id = fdw_id_var;
    END IF;
END;
$$;


DROP FUNCTION IF EXISTS __pg_springtail_triggers.table_owner_remove;
/**
 * This function removes the table owner snapshot history for a given fdw_id and table_oid.
 * If fdw_id_var is NULL, it removes all entries for that table_oid across all fdw_ids.
 */
CREATE OR REPLACE FUNCTION __pg_springtail_triggers.table_owner_remove(fdw_id_var TEXT, table_oid_var OID)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO __pg_springtail_triggers
AS $$
BEGIN
    IF fdw_id_var IS NULL THEN
        -- Throw an error if fdw_id_var is NULL, as we cannot remove entries without a specific fdw_id
        RAISE EXCEPTION 'fdw_id_var cannot be NULL. Please provide a valid fdw_id.';
    ELSE
        -- Remove the specific entry for the given fdw_id and table_oid
        DELETE FROM __pg_springtail_triggers.table_owner_snapshot_history
        WHERE fdw_id = fdw_id_var AND table_oid = table_oid_var;
    END IF;
END;
$$;


DROP FUNCTION IF EXISTS __pg_springtail_triggers.table_owner_diff;
/**
 * This function compares the current state of table ownership with the last snapshot
 * and returns the differences as a table for a given fdw_id.
 * It returns a table with the following columns:
 * - diff_type: 'ADDED', 'REMOVED', or 'MODIFIED'
 * - table_oid: OID of the table
 * - table_name: Name of the table
 * - schema_name: Name of the schema the table belongs to
 * - role_owner_oid: OID of the table owner
 * - role_owner_name: Name of the table owner
 */
CREATE OR REPLACE FUNCTION __pg_springtail_triggers.table_owner_diff(fdw_id_var TEXT)
RETURNS TABLE (
    diff_type TEXT,
    table_oid OID,
    table_name TEXT,
    schema_name TEXT,
    role_owner_oid OID,
    role_owner_name TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO __pg_springtail_triggers, pg_catalog, pg_temp
AS $$
DECLARE
    v_current_snapshot_time TIMESTAMPTZ := clock_timestamp();
    v_last_snapshot_time TIMESTAMPTZ;
 BEGIN
    -- 1. Create a temporary table for the current snapshot of table owners
    DROP TABLE IF EXISTS __pg_springtail_current_table_owner_snapshot;
    CREATE TEMPORARY TABLE __pg_springtail_current_table_owner_snapshot (
        table_oid OID PRIMARY KEY,
        table_name TEXT NOT NULL,
        schema_name TEXT NOT NULL,
        schema_oid OID NOT NULL,
        role_owner_oid OID NOT NULL,
        role_owner_name TEXT NOT NULL
    ) ON COMMIT DROP;

    -- Populate the temporary table with the current state of table ownership
    INSERT INTO __pg_springtail_current_table_owner_snapshot
    SELECT
        c.oid AS table_oid,
        c.relname AS table_name,
        n.nspname AS schema_name,
        n.oid AS schema_oid,
        c.relowner AS role_owner_oid,
        r.rolname AS role_owner_name
    FROM
        pg_class c
    JOIN
        pg_namespace n ON n.oid = c.relnamespace
    JOIN
        pg_roles r ON c.relowner = r.oid
    WHERE
        c.relkind = 'r' AND
        c.relrowsecurity IS TRUE AND
        n.nspname NOT LIKE 'pg_temp_%' AND
        n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast', '__pg_springtail_triggers');

    -- 2. Determine the last snapshot time from the history table
    SELECT MAX(snapshot_time) INTO v_last_snapshot_time
      FROM __pg_springtail_triggers.table_owner_snapshot_history
      WHERE fdw_id = fdw_id_var;

    -- 3. If no history exists, populate it and return snapshot
    IF v_last_snapshot_time IS NULL THEN
        INSERT INTO __pg_springtail_triggers.table_owner_snapshot_history
        SELECT
            fdw_id_var,
            s.table_oid,
            s.table_name,
            s.schema_name,
            s.schema_oid,
            s.role_owner_oid,
            s.role_owner_name,
            v_current_snapshot_time
        FROM
            __pg_springtail_current_table_owner_snapshot s;

        RETURN QUERY
        SELECT
            'ADDED' AS diff_type,
            s.table_oid,
            s.table_name,
            s.schema_name,
            s.role_owner_oid,
            s.role_owner_name
        FROM
            __pg_springtail_current_table_owner_snapshot s;
        RETURN;
    END IF;

    -- 4. If history exists, perform the Full Outer Join comparison
    RETURN QUERY
    SELECT
        CASE
            WHEN cur.table_oid IS NULL THEN 'REMOVED'
            WHEN prev.table_oid IS NULL THEN 'ADDED'
            ELSE 'MODIFIED'
        END AS diff_type,
        COALESCE(cur.table_oid, prev.table_oid) AS table_oid,
        COALESCE(cur.table_name, prev.table_name) AS table_name,
        COALESCE(cur.schema_name, prev.schema_name) AS schema_name,
        COALESCE(cur.role_owner_oid, prev.role_owner_oid) AS role_owner_oid,
        COALESCE(cur.role_owner_name, prev.role_owner_name) AS role_owner_name
    FROM
        __pg_springtail_current_table_owner_snapshot AS cur
    FULL OUTER JOIN
    (
        SELECT *
        FROM __pg_springtail_triggers.table_owner_snapshot_history
        WHERE fdw_id = fdw_id_var
    ) AS prev
    ON (cur.table_oid = prev.table_oid)
    WHERE
        cur.table_oid IS NULL
        OR prev.table_oid IS NULL
        OR cur.role_owner_oid IS DISTINCT FROM prev.role_owner_oid
    ORDER BY
        COALESCE(cur.table_oid, prev.table_oid), COALESCE(cur.role_owner_oid, prev.role_owner_oid);

    -- 5. Update the history table with the current snapshot
    DELETE FROM __pg_springtail_triggers.table_owner_snapshot_history
        WHERE fdw_id = fdw_id_var;

    INSERT INTO __pg_springtail_triggers.table_owner_snapshot_history
    SELECT
        fdw_id_var,
        s.table_oid,
        s.table_name,
        s.schema_name,
        s.schema_oid,
        s.role_owner_oid,
        s.role_owner_name,
        v_current_snapshot_time
    FROM
        __pg_springtail_current_table_owner_snapshot s;
END;
$$;