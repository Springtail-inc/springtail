CREATE SCHEMA IF NOT EXISTS __pg_springtail_triggers;

DROP TABLE IF EXISTS __pg_springtail_triggers.policy_snapshot_history;
CREATE TABLE IF NOT EXISTS __pg_springtail_triggers.policy_snapshot_history (
    fdw_id TEXT NOT NULL,
    policy_oid OID NOT NULL,
    table_oid OID NOT NULL,
    table_name TEXT NOT NULL,
    schema_name TEXT NOT NULL,
    schema_oid OID NOT NULL,
    policy_name TEXT NOT NULL,
    policy_permissive BOOLEAN,
    policy_roles TEXT[],
    policy_qual TEXT,
    snapshot_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (fdw_id, policy_oid)
);


DROP FUNCTION IF EXISTS __pg_springtail_triggers.reset_policy_diff;

/**
 * This function resets the policy snapshot history for a given fdw_id.
 * If fdw_id_var is NULL, it truncates the entire history table.
 * If fdw_id_var is provided, it deletes only the entries for that specific fdw_id.
 */
CREATE OR REPLACE FUNCTION __pg_springtail_triggers.reset_policy_diff(fdw_id_var TEXT)
RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO __pg_springtail_triggers
AS $$
BEGIN
    -- If fdw_id_var is NULL, it indicates a request to reset the entire role snapshot history,
    -- so the entire table is truncated to remove all entries.
    IF fdw_id_var IS NULL THEN
        TRUNCATE __pg_springtail_triggers.policy_snapshot_history;
    ELSE
        -- Reset the policy snapshot history by truncating the table
        DELETE FROM __pg_springtail_triggers.policy_snapshot_history
            WHERE fdw_id = fdw_id_var;
    END IF;
END;
$$;

DROP FUNCTION IF EXISTS __pg_springtail_triggers.policy_remove;
/**
 * This function removes the policy snapshot history for a given fdw_id and policy_oid.
 * If fdw_id_var is NULL, it removes all entries for that policy_oid across all fdw_ids.
 */
CREATE OR REPLACE FUNCTION __pg_springtail_triggers.policy_remove(fdw_id_var TEXT, policy_oid_var OID)
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
        -- Remove the specific entry for the given fdw_id and policy_oid
        DELETE FROM __pg_springtail_triggers.policy_snapshot_history
        WHERE fdw_id = fdw_id_var AND policy_oid = policy_oid_var;
    END IF;
END;
$$;

/** Drop the function if it exists to allow easy re-creation during development */
DROP FUNCTION IF EXISTS __pg_springtail_triggers.policy_diff;

/**
 * This function compares the current state of policies with the last snapshot
 * and returns the differences as a table for a given fdw_id.
 * It returns a table with the following columns:
 * - diff_type: 'ADDED', 'REMOVED', or 'MODIFIED'
 * - table_oid: OID of the table the policy is associated with
 * - table_name: Name of the table the policy is associated with
 * - schema_name: Name of the schema the table belongs to
 * - policy_oid: OID of the policy
 * - policy_name: Name of the policy
 * - policy_name_old: Previous name of the policy (if modified)
 * - policy_permissive: Whether the policy is permissive or restrictive
 * - policy_roles: Roles that the policy applies to, as a JSON array
 * - policy_qual: The USING clause of the policy, if any
 */
CREATE OR REPLACE FUNCTION __pg_springtail_triggers.policy_diff(fdw_id_var TEXT)
RETURNS TABLE (
    diff_type TEXT,
    table_oid OID,
    table_name TEXT,
    schema_name TEXT,
    policy_oid OID,
    policy_name TEXT,
    policy_name_old TEXT,
    policy_permissive BOOLEAN,
    policy_roles TEXT,
    policy_qual TEXT
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO __pg_springtail_triggers, pg_catalog, pg_temp
AS $$
DECLARE
    v_current_snapshot_time TIMESTAMPTZ := clock_timestamp();
    v_last_snapshot_time TIMESTAMPTZ;
BEGIN
    -- 1. Create a temporary table for the current snapshot of policies
    -- Must exactly match the structure of the history table to ensure compatibility
    DROP TABLE IF EXISTS __pg_springtail_current_policy_snapshot; -- Clean up any previous temporary table
    CREATE TEMPORARY TABLE __pg_springtail_current_policy_snapshot (
        policy_oid OID PRIMARY KEY,
        table_oid OID NOT NULL,
        table_name TEXT NOT NULL,
        schema_name TEXT NOT NULL,
        schema_oid OID NOT NULL,
        policy_name TEXT NOT NULL,
        policy_permissive BOOLEAN,
        policy_roles TEXT[],
        policy_qual TEXT
    ) ON COMMIT DROP; -- Ensures the temporary table is dropped at the end of the transaction

    -- Populate the temporary table with the current state of individual policies
    INSERT INTO __pg_springtail_current_policy_snapshot
    SELECT
        po.oid AS policy_oid,         -- Actual policy OID from pg_policy
        c.oid AS table_oid,
        c.relname AS table_name,
        n.nspname AS schema_name,
        n.oid AS schema_oid,
        p.policyname AS policy_name,
        po.polpermissive AS policy_permissive,
        p.roles AS policy_roles,
        p.qual AS policy_qual
    FROM
        pg_class AS c
    JOIN
        pg_namespace AS n ON n.oid = c.relnamespace
    JOIN -- Use INNER JOIN here as we only care about tables with actual policies
        pg_policy AS po ON po.polrelid = c.oid
    JOIN
        pg_policies AS p
        ON (p.schemaname = n.nspname AND p.tablename = c.relname AND p.policyname = po.polname)
    WHERE
        c.relkind = 'r' -- Regular tables
        AND polcmd IN ('r', '*') -- Only consider policies that are applicable for SELECT or all commands
        AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast');

    -- 2. Determine the last snapshot time from the history table
    SELECT MAX(snapshot_time) INTO v_last_snapshot_time
      FROM __pg_springtail_triggers.policy_snapshot_history
      WHERE fdw_id_var = fdw_id; -- Filter by fdw_id to ensure we only consider relevant snapshots

    -- 3. If no history exists, populate it and return snapshot
    IF v_last_snapshot_time IS NULL THEN
        INSERT INTO __pg_springtail_triggers.policy_snapshot_history
        SELECT
            fdw_id_var,
            ps.policy_oid,
            ps.table_oid,
            ps.table_name,
            ps.schema_name,
            ps.schema_oid,
            ps.policy_name,
            ps.policy_permissive,
            ps.policy_roles,
            ps.policy_qual,
            v_current_snapshot_time
        FROM
            __pg_springtail_current_policy_snapshot as ps;

        -- Return the initial snapshot as there is no previous snapshot to compare against
        RETURN QUERY
        SELECT
            'ADDED' AS diff_type,
            ps.table_oid,
            ps.table_name,
            ps.schema_name,
            ps.policy_oid,
            ps.policy_name,
            NULL AS policy_name_old,
            ps.policy_permissive,
            array_to_json(ps.policy_roles)::text AS policy_roles,
            ps.policy_qual
        FROM
            __pg_springtail_current_policy_snapshot as ps;
        RETURN; -- Exit the function after returning the initial snapshot
    END IF;

    -- 4. If history exists, perform the Full Outer Join comparison
    RETURN QUERY
    SELECT
        CASE
            WHEN cur.policy_oid IS NULL THEN 'REMOVED' -- Exists in old, not in current
            WHEN prev.policy_oid IS NULL THEN 'ADDED'  -- Exists in current, not in old
            ELSE 'MODIFIED'                            -- Exists in both, but attributes differ
        END AS diff_type,
        COALESCE(cur.table_oid, prev.table_oid) AS table_oid,
        COALESCE(cur.table_name, prev.table_name) AS table_name,
        COALESCE(cur.schema_name, prev.schema_name) AS schema_name,
        COALESCE(cur.policy_oid, prev.policy_oid) AS policy_oid,
        COALESCE(cur.policy_name, prev.policy_name) AS policy_name,
        prev.policy_name AS policy_name_old,
        cur.policy_permissive AS policy_permissive,
        array_to_json(cur.policy_roles)::text AS policy_roles,
        cur.policy_qual AS policy_qual  -- for USING clause
    FROM
        __pg_springtail_current_policy_snapshot AS cur
    FULL OUTER JOIN
        __pg_springtail_triggers.policy_snapshot_history AS prev
        ON (cur.policy_oid = prev.policy_oid AND prev.fdw_id = fdw_id_var)
    WHERE
        cur.policy_oid IS NULL -- Policy was removed
        OR prev.policy_oid IS NULL -- Policy was added
        OR ( -- Policy was modified
            cur.policy_permissive IS DISTINCT FROM prev.policy_permissive OR
            cur.policy_roles IS DISTINCT FROM prev.policy_roles OR
            cur.policy_qual IS DISTINCT FROM prev.policy_qual OR
            cur.policy_name IS DISTINCT FROM prev.policy_name -- Policy name might change even if OID is same, though rare for RLS
        )
        ORDER BY COALESCE(cur.policy_oid, prev.policy_oid);

    -- 5. Update the history table with the current snapshot
    -- First, delete the old snapshot(s) to maintain only the latest
    DELETE FROM policy_snapshot_history
        WHERE fdw_id = fdw_id_var;

    -- Then, insert the new snapshot
    INSERT INTO __pg_springtail_triggers.policy_snapshot_history
    SELECT
        fdw_id_var,
        ps.policy_oid,
        ps.table_oid,
        ps.table_name,
        ps.schema_name,
        ps.schema_oid,
        ps.policy_name,
        ps.policy_permissive,
        ps.policy_roles,
        ps.policy_qual,
        v_current_snapshot_time
    FROM
        __pg_springtail_current_policy_snapshot as ps;
END;
$$;

/* --Test
truncate __pg_springtail_triggers.policy_snapshot_history;
select __pg_springtail_triggers.policy_diff('0');
select __pg_springtail_triggers.policy_diff('1');


-- Step 1: Create the table
CREATE TABLE my_table (
    id INTEGER,
    data TEXT
);

-- Step 2: Enable row-level security
ALTER TABLE my_table ENABLE ROW LEVEL SECURITY;

-- Step 3: Create a policy that filters rows where id = 3
CREATE POLICY id_equals_3_policy
    ON my_table
    FOR SELECT
    USING (id = 3);
*/