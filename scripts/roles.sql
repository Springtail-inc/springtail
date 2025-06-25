-- filepath: /Users/garth/src/springtail2/scripts/roles.sql
CREATE SCHEMA IF NOT EXISTS __pg_springtail_triggers;

DROP TABLE IF EXISTS __pg_springtail_triggers.role_snapshot_history;
CREATE TABLE IF NOT EXISTS __pg_springtail_triggers.role_snapshot_history (
    fdw_id TEXT NOT NULL,
    role_oid OID NOT NULL,
    rolname TEXT NOT NULL,
    rolsuper BOOLEAN,
    rolinherit BOOLEAN,
    rolcanlogin BOOLEAN,
    rolbypassrls BOOLEAN,
    snapshot_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (fdw_id, role_oid)
);

DROP FUNCTION IF EXISTS __pg_springtail_triggers.reset_role_diff;

/**
 * This function resets the role snapshot history for a given fdw_id.
 * If fdw_id_var is NULL, it truncates the entire history table.
 * If fdw_id_var is provided, it deletes only the entries for that specific fdw_id.
 */
CREATE OR REPLACE FUNCTION __pg_springtail_triggers.reset_role_diff(fdw_id_var TEXT)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO __pg_springtail_triggers, pg_catalog, pg_temp
AS $$
BEGIN
    -- If fdw_id_var is NULL, it indicates a request to reset the entire role snapshot history,
    -- so the entire table is truncated to remove all entries.
    IF fdw_id_var IS NULL THEN
        TRUNCATE __pg_springtail_triggers.role_snapshot_history;
    ELSE
        DELETE FROM __pg_springtail_triggers.role_snapshot_history
            WHERE fdw_id = fdw_id_var;
    END IF;
END;
$$;

DROP FUNCTION IF EXISTS __pg_springtail_triggers.role_diff;

/**
 * This function compares the current state of roles with the last snapshot
 * and returns the differences as a table for a given fdw_id.
 * It returns a table with the following columns:
 * - diff_type: 'ADDED', 'REMOVED', or 'MODIFIED'
 * - role_oid: OID of the role
 * - rolname: Name of the role
 * - rolsuper: Whether the role is a superuser
 * - rolinherit: Whether the role inherits privileges
 * - rolcanlogin: Whether the role can login
 * - rolbypassrls: Whether the role bypasses row-level security
 */
CREATE OR REPLACE FUNCTION __pg_springtail_triggers.role_diff(fdw_id_var TEXT)
RETURNS TABLE (
    diff_type TEXT,
    role_oid OID,
    rolname TEXT,
    rolsuper BOOLEAN,
    rolinherit BOOLEAN,
    rolcanlogin BOOLEAN,
    rolbypassrls BOOLEAN
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO __pg_springtail_triggers, pg_catalog, pg_temp
AS $$
DECLARE
    v_current_snapshot_time TIMESTAMPTZ := clock_timestamp();
    v_last_snapshot_time TIMESTAMPTZ;
BEGIN
    -- 1. Create a temporary table for the current snapshot of roles
    DROP TABLE IF EXISTS __pg_springtail_current_role_snapshot;
    CREATE TEMPORARY TABLE __pg_springtail_current_role_snapshot (
        role_oid OID PRIMARY KEY,
        rolname TEXT NOT NULL,
        rolsuper BOOLEAN,
        rolinherit BOOLEAN,
        rolcanlogin BOOLEAN,
        rolbypassrls BOOLEAN
    ) ON COMMIT DROP;

    -- Populate the temporary table with the current state of roles
    INSERT INTO __pg_springtail_current_role_snapshot
    SELECT
        r.oid AS role_oid,
        r.rolname,
        r.rolsuper,
        r.rolinherit,
        r.rolcanlogin,
        r.rolbypassrls
    FROM
        pg_roles AS r
    WHERE
        -- NOTE: these are current as of Postgres 18 beta
        r.rolname NOT IN (
            'pg_database_owner',
            'pg_read_all_data',
            'pg_write_all_data',
            'pg_monitor',
            'pg_read_all_settings',
            'pg_read_all_stats',
            'pg_stat_scan_tables',
            'pg_read_server_files',
            'pg_write_server_files',
            'pg_execute_server_program',
            'pg_signal_backend',
            'pg_checkpoint',
            'pg_maintain',
            'pg_use_reserved_connections',
            'pg_create_subscription',
            'pg_signal_autovacuum_worker');

    -- 2. Determine the last snapshot time from the history table
    SELECT MAX(snapshot_time) INTO v_last_snapshot_time
      FROM __pg_springtail_triggers.role_snapshot_history
      WHERE fdw_id = fdw_id_var;

    -- 3. If no history exists, populate it and return snapshot
    IF v_last_snapshot_time IS NULL THEN
        INSERT INTO __pg_springtail_triggers.role_snapshot_history
        SELECT
            fdw_id_var,
            rs.role_oid,
            rs.rolname,
            rs.rolsuper,
            rs.rolinherit,
            rs.rolcanlogin,
            rs.rolbypassrls,
            v_current_snapshot_time
        FROM
            __pg_springtail_current_role_snapshot as rs;

        RETURN QUERY
        SELECT
            'ADDED' AS diff_type,
            rs.role_oid,
            rs.rolname,
            rs.rolsuper,
            rs.rolinherit,
            rs.rolcanlogin,
            rs.rolbypassrls
        FROM
            __pg_springtail_current_role_snapshot as rs;
        RETURN;
    END IF;

    -- 4. If history exists, perform the Full Outer Join comparison
    RETURN QUERY
    SELECT
        CASE
            WHEN cur.role_oid IS NULL THEN 'REMOVED'
            WHEN prev.role_oid IS NULL THEN 'ADDED'
            ELSE 'MODIFIED'
        END AS diff_type,
        COALESCE(cur.role_oid, prev.role_oid) AS role_oid,
        COALESCE(cur.rolname, prev.rolname) AS rolname,
        cur.rolsuper,
        cur.rolinherit,
        cur.rolcanlogin,
        cur.rolbypassrls
    FROM
        __pg_springtail_current_role_snapshot AS cur
    FULL OUTER JOIN
        __pg_springtail_triggers.role_snapshot_history AS prev ON (cur.role_oid = prev.role_oid)
    WHERE
        prev.fdw_id = fdw_id_var
        AND (
            cur.role_oid IS NULL -- Role was removed
            OR prev.role_oid IS NULL -- Role was added
            OR (
                cur.rolsuper IS DISTINCT FROM prev.rolsuper OR
                cur.rolinherit IS DISTINCT FROM prev.rolinherit OR
                cur.rolcanlogin IS DISTINCT FROM prev.rolcanlogin OR
                cur.rolbypassrls IS DISTINCT FROM prev.rolbypassrls OR
                cur.rolname IS DISTINCT FROM prev.rolname
            )
        );

    -- 5. Update the history table with the current snapshot
    DELETE FROM role_snapshot_history
        WHERE fdw_id = fdw_id_var;

    INSERT INTO __pg_springtail_triggers.role_snapshot_history
    SELECT
        fdw_id_var,
        rs.role_oid,
        rs.rolname,
        rs.rolsuper,
        rs.rolinherit,
        rs.rolcanlogin,
        rs.rolbypassrls,
        v_current_snapshot_time
    FROM
        __pg_springtail_current_role_snapshot as rs;
END;
$$;

/* --Test
truncate __pg_springtail_triggers.role_snapshot_history;
select __pg_springtail_triggers.role_diff('0');
select __pg_springtail_triggers.role_diff('1');

-- Example: Create a new role
CREATE ROLE my_test_role LOGIN SUPERUSER;
*/