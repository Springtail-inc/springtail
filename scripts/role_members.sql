-- Sync role memberships in Postgres
-- Omit roles that are system-defined and omit members of those roles
CREATE SCHEMA IF NOT EXISTS __pg_springtail_triggers;

DROP TABLE IF EXISTS __pg_springtail_triggers.role_member_snapshot_history;
CREATE TABLE IF NOT EXISTS __pg_springtail_triggers.role_member_snapshot_history (
    fdw_id TEXT NOT NULL,
    role_oid OID NOT NULL,
    member_oid OID NOT NULL,
    role_name TEXT NOT NULL,
    member_name TEXT NOT NULL,
    snapshot_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (fdw_id, role_oid, member_oid)
);

DROP FUNCTION IF EXISTS __pg_springtail_triggers.reset_role_member_diff;

CREATE OR REPLACE FUNCTION __pg_springtail_triggers.reset_role_member_diff(fdw_id_var TEXT)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO __pg_springtail_triggers, pg_catalog, pg_temp
AS $$
BEGIN
    IF fdw_id_var IS NULL THEN
        TRUNCATE __pg_springtail_triggers.role_member_snapshot_history;
    ELSE
        DELETE FROM __pg_springtail_triggers.role_member_snapshot_history
            WHERE fdw_id = fdw_id_var;
    END IF;
END;
$$;

DROP FUNCTION IF EXISTS __pg_springtail_triggers.role_member_diff;

CREATE OR REPLACE FUNCTION __pg_springtail_triggers.role_member_diff(fdw_id_var TEXT)
RETURNS TABLE (
    diff_type TEXT,
    role_oid OID,
    member_oid OID,
    role_name TEXT,
    member_name TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO __pg_springtail_triggers, pg_catalog, pg_temp
AS $$
DECLARE
    v_current_snapshot_time TIMESTAMPTZ := clock_timestamp();
    v_last_snapshot_time TIMESTAMPTZ;
BEGIN
    -- 1. Create a temporary table for the current snapshot of role memberships
    DROP TABLE IF EXISTS __pg_springtail_current_role_member_snapshot;
    CREATE TEMPORARY TABLE __pg_springtail_current_role_member_snapshot (
        role_oid OID NOT NULL,
        member_oid OID NOT NULL,
        role_name TEXT NOT NULL,
        member_name TEXT NOT NULL,
        PRIMARY KEY (role_oid, member_oid)
    ) ON COMMIT DROP;

    -- Populate the temporary table with the current state of role memberships
    INSERT INTO __pg_springtail_current_role_member_snapshot
    SELECT
        r.oid AS role_oid,
        m.member AS member_oid,
        r.rolname AS role_name,
        u.rolname AS member_name
    FROM
        pg_roles AS r
    JOIN
        pg_auth_members AS m ON r.oid = m.roleid
    JOIN
        pg_roles AS u ON m.member = u.oid
    WHERE
        -- Note: these are up-to-date as of Postgres 18 beta
        u.rolname NOT IN (
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
            'pg_signal_autovacuum_worker')
        AND r.rolname NOT IN (
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
      FROM __pg_springtail_triggers.role_member_snapshot_history
      WHERE fdw_id = fdw_id_var;

    -- 3. If no history exists, populate it and return snapshot
    IF v_last_snapshot_time IS NULL THEN
        INSERT INTO __pg_springtail_triggers.role_member_snapshot_history
        SELECT
            fdw_id_var,
            rs.role_oid,
            rs.member_oid,
            rs.role_name,
            rs.member_name,
            v_current_snapshot_time
        FROM
            __pg_springtail_current_role_member_snapshot as rs;

        RETURN QUERY
        SELECT
            'ADDED' AS diff_type,
            rs.role_oid,
            rs.member_oid,
            rs.role_name,
            rs.member_name
        FROM
            __pg_springtail_current_role_member_snapshot as rs;
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
        COALESCE(cur.member_oid, prev.member_oid) AS member_oid,
        COALESCE(cur.role_name, prev.role_name) AS role_name,
        COALESCE(cur.member_name, prev.member_name) AS member_name
    FROM
        __pg_springtail_current_role_member_snapshot AS cur
    FULL OUTER JOIN
        __pg_springtail_triggers.role_member_snapshot_history AS prev
        ON cur.role_oid = prev.role_oid AND cur.member_oid = prev.member_oid
    WHERE
        prev.fdw_id = fdw_id_var
        AND (
            cur.role_oid IS NULL -- Membership was removed
            OR prev.role_oid IS NULL -- Membership was added
        );

    -- 5. Update the history table with the current snapshot
    DELETE FROM role_member_snapshot_history
        WHERE fdw_id = fdw_id_var;

    INSERT INTO __pg_springtail_triggers.role_member_snapshot_history
    SELECT
        fdw_id_var,
        rs.role_oid,
        rs.member_oid,
        rs.role_name,
        rs.member_name,
        v_current_snapshot_time
    FROM
        __pg_springtail_current_role_member_snapshot as rs;
END;
$$;
