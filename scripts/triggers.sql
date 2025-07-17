-- Triggers for create/alter table and drop table events
-- https://www.postgresql.org/docs/current/plpgsql-trigger.html
CREATE SCHEMA IF NOT EXISTS __pg_springtail_triggers;

CREATE OR REPLACE FUNCTION __pg_springtail_triggers.springtail_event_trigger_for_drops()
        RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj record;
    msg json;
    tag_name text;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        -- Check for table or index drops
        IF NOT obj.is_temporary AND (obj.object_type = 'table'
                                     OR obj.object_type = 'index'
                                     OR obj.object_type = 'schema'
                                     OR obj.object_type = 'type')
            AND (obj.schema_name IS NULL OR obj.schema_name NOT LIKE 'pg_%') THEN

            -- sometimes tg_tag is DROP TABLE even if type is index
            IF obj.object_type = 'table' THEN
                tag_name := 'DROP TABLE';
            ELSIF obj.object_type = 'index' THEN
                tag_name := 'DROP INDEX';
            ELSIF obj.object_type = 'schema' THEN
                tag_name := 'DROP SCHEMA';
            ELSIF obj.object_type = 'type' THEN
                -- the drop has already been done, so no way to check the type
                -- seems drop type does two drops, one for the type and one for the enum labels
                -- ignore the enum labels, if obj.object_identity contains []
                -- RAISE NOTICE 'springtail: drop type % % %', obj.schema_name, obj.object_name, obj.object_identity;
                IF (obj.object_identity LIKE '%[]' AND obj.object_name LIKE '_%')
                    OR obj.original IS FALSE OR obj.schema_name LIKE 'pg_%' THEN
                    CONTINUE;
                END IF;
                tag_name := 'DROP TYPE';
            END IF;

            -- generate message same for DROP TABLE/INDEX
            msg := json_build_object('cmd', tag_name,
                'oid', obj.objid::bigint, -- oid is unsigned int, but comes as string
                'obj', obj.object_type,
                'schema', obj.schema_name,
                'name', obj.object_name,
                'identity', obj.object_identity);

            -- RAISE NOTICE 'springtail: % op, %', tag_name, msg::text;

            -- tag_name is DROP TABLE or DROP INDEX or DROP SCHEMA
            PERFORM pg_logical_emit_message(true, 'springtail:' || tag_name, msg::text);
        END IF;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION __pg_springtail_triggers.springtail_get_partition_data(table_name TEXT)
RETURNS JSON LANGUAGE plpgsql AS $$
DECLARE
BEGIN
        RETURN (
        SELECT json_agg(json_col)
        FROM (
            SELECT json_build_object(
                'table_name', obj_select.table_name,
                'table_id', obj_select.table_id::bigint,
                'namespace_name', obj_select.namespace_name,
                'namespace_id', obj_select.namespace_id::bigint,
                'partition_bound', obj_select.partition_bound,
                'partition_key', obj_select.partition_key,
                'parent_table_id', obj_select.parent_table_id::bigint
            ) AS json_col
            FROM (
                WITH RECURSIVE children AS (
                    SELECT inhrelid, inhparent
                    FROM pg_inherits
                    WHERE inhparent = table_name::regclass
                    UNION ALL
                    SELECT pi.inhrelid, pi.inhparent
                    FROM pg_inherits pi
                    JOIN children c ON c.inhrelid = pi.inhparent
                )
                SELECT
                    child.relname AS table_name,
                    child.oid AS table_id,
                    child_ns.nspname AS namespace_name,
                    child_ns.oid AS namespace_id,
                    pg_get_expr(child.relpartbound, child.oid, TRUE) AS partition_bound,
                    pg_get_partkeydef(child.oid) AS partition_key,
                    children.inhparent AS parent_table_id
                FROM children
                JOIN pg_class child ON child.oid = children.inhrelid
                JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
            ) AS obj_select
        ) AS json_columns
    );
END;
$$;

-- Handle partition events
CREATE OR REPLACE FUNCTION __pg_springtail_triggers.springtail_handle_partition_events(
    table_id oid,
    table_name TEXT,
    schema_name TEXT,
    partition_key TEXT,
    partition_data JSON
)
    RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    command_text text;
    msg text;
BEGIN
    command_text := current_query();

    -- Handle the detach partition event
    IF position('detach partition' IN lower(command_text)) > 0 THEN
        msg := json_build_object('xid', txid_current(),
            'cmd', 'DETACH PARTITION',
            'table_id', table_id::bigint,
            'schema', schema_name,
            'table', table_name,
            'partition_key', partition_key,
            'partition_data', partition_data);

        PERFORM pg_logical_emit_message(true, 'springtail:' || 'DETACH PARTITION', msg::text);
        RETURN TRUE;
    END IF;

    -- Handle the attach partition event
    IF position('attach partition' IN lower(command_text)) > 0 THEN
        msg := json_build_object('xid', txid_current(),
            'cmd', 'ATTACH PARTITION',
            'table_id', table_id::bigint,
            'schema', schema_name,
            'table', table_name,
            'partition_key', partition_key,
            'partition_data', partition_data);

        PERFORM pg_logical_emit_message(true, 'springtail:' || 'ATTACH PARTITION', msg::text);
        RETURN TRUE;
    END IF;

    RETURN FALSE;
END;
$$;

-- Handle index events
CREATE OR REPLACE FUNCTION __pg_springtail_triggers.springtail_handle_index_events(
    obj_id oid,
    object_identity text,
    object_type text,
    command_tag text,
    schema_name text
)
    RETURNS void  LANGUAGE plpgsql AS $$
DECLARE
    ind_obj record;
    json_columns json;
    msg text;
BEGIN
    EXECUTE format('SELECT
            c.oid AS table_oid,
            c.relname AS table_name,
            i.indisunique AS is_unique,
            i.indisprimary AS primary_idx,
            i.indkey AS indkey
        FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
        WHERE i.indexrelid = %s', obj_id) INTO ind_obj;

    IF ind_obj.primary_idx IS true THEN
        RETURN;
    END IF;

    -- get index columns
    SELECT json_agg(json_col)
    FROM (
        SELECT json_build_object('name', pga.attname,
            'position', pga.attnum,
            'idx_position', array_position(ind_obj.indkey, pga.attnum)
        ) AS json_col
        FROM pg_attribute pga
        WHERE
            pga.attrelid=ind_obj.table_oid
            AND (array_position(ind_obj.indkey, pga.attnum) IS NOT NULL)
            AND attisdropped=false
    ) AS obj_select
    INTO json_columns;

    -- build msg json object
    msg := json_build_object('xid', txid_current(),
        'cmd', command_tag,
        'oid', obj_id::bigint,
        'obj', object_type,
        'schema', schema_name,
        'identity', object_identity,
        'table_oid', ind_obj.table_oid::bigint,
        'table_name', ind_obj.table_name,
        'is_unique', ind_obj.is_unique,
        'columns', json_columns);

    -- command_tag is CREATE INDEX
    -- RAISE NOTICE 'springtail: % op, %', obj.command_tag, msg::text;
    PERFORM pg_logical_emit_message(true, 'springtail:' || command_tag, msg::text);
END;
$$;

-- Handle table events
CREATE OR REPLACE FUNCTION __pg_springtail_triggers.springtail_handle_table_events(
    obj_id oid,
    object_identity text,
    command_tag text,
    schema_name text
)
    RETURNS json LANGUAGE plpgsql AS $$
DECLARE
    -- Table meta
    table_relname text;
    table_namespace_id oid;
    table_replident "char";
    table_persistence "char";
    rel_kind "char";
    has_pkey boolean;
    json_columns json;
    -- Partition details
    parent_table_id oid;
    partition_bound text;
    partition_key text;
    partition_data json;
    is_partition_event boolean;
    -- Table details output
    table_info text;
BEGIN
    -- Get the table details from pg_class along with the partition details
    SELECT pg_class.relname, pg_class.relnamespace, pg_class.relreplident, pg_class.relpersistence, pg_class.relkind, CASE WHEN pg_class.relispartition THEN
            (SELECT inhparent FROM pg_inherits WHERE inhrelid = pg_class.oid)
        END as parent_table_id,
        pg_get_expr(pg_class.relpartbound, pg_class.oid, TRUE) as partition_bound,
        pg_get_partkeydef(pg_class.oid) as partition_key
    FROM pg_class
    WHERE oid = obj_id
    INTO table_relname, table_namespace_id, table_replident, table_persistence, rel_kind, parent_table_id, partition_bound, partition_key;

    -- Only during the ALTER command, get the partition details. This is required to handle the partition events
    IF command_tag = 'ALTER TABLE' AND partition_key IS NOT NULL THEN
        SELECT __pg_springtail_triggers.springtail_get_partition_data(object_identity) INTO partition_data;

        -- Handler for attach and detach partition events
        SELECT __pg_springtail_triggers.springtail_handle_partition_events(
            obj_id,
            table_relname,
            schema_name,
            partition_key,
            partition_data
        ) INTO is_partition_event;

        IF is_partition_event IS TRUE THEN
            RETURN NULL;
        END IF;
    END IF;

    -- This is a corner case when an index is renamed through "ALTER TABLE" statement
    -- In this case our object is an index, not a table. So, we can't do anything with it here.
    -- 'i' - normal index, 'I' - partitioned index
    -- 'r' - normal table, 'p' - partitioned table
    IF rel_kind <> 'r' AND rel_kind <> 'p' THEN
        RETURN NULL;
    END IF;

    SELECT json_agg(json_col)
    FROM (
        SELECT json_build_object('name', column_name,
            'is_nullable', is_nullable::boolean,
            'pg_type', atttypid::int,
            'default', column_default,
            'is_pkey', coalesce((pga.attnum=any(pgi.indkey))::boolean, false),
            'position', ordinal_position,
            'pkey_pos', array_position(pgi.indkey, pga.attnum),
            'is_generated', (pga.attgenerated = 's')::boolean,
            'type_name', t.typname,
            'collation', col.collname,
            'is_user_defined_type', (t.typnamespace <> 'pg_catalog'::regnamespace AND t.typnamespace <> 'information_schema'::regnamespace)::boolean,
            'is_non_standard_collation', coalesce((col.collname NOT IN ('C', 'en_US.UTF-8', 'default'))::boolean, false),
            'type_category', t.typcategory,
            'type_namespace', nsp.nspname
        ) AS json_col
        FROM pg_attribute pga
        JOIN information_schema.columns
        ON column_name=pga.attname
        LEFT OUTER JOIN pg_index pgi
        ON pga.attrelid=pgi.indrelid AND pgi.indisprimary
        LEFT JOIN pg_type t ON pga.atttypid = t.oid
        LEFT JOIN pg_collation col ON pga.attcollation = col.oid AND pga.attcollation <> 0
        LEFT JOIN pg_catalog.pg_namespace nsp ON nsp.oid = t.typnamespace
        WHERE pga.attrelid=obj_id
            AND quote_literal(table_schema) = quote_literal(schema_name)
            AND quote_literal(table_name) = quote_literal(table_relname)
            AND atttypid > 0
        ORDER BY ordinal_position
    ) AS obj_select
    INTO json_columns;

    SELECT true WHERE json_columns::jsonb @> '[{"is_pkey": true}]'::jsonb INTO has_pkey;

    -- If a table is created or altered, and it doesn't have a primary key, set REPLICA IDENTITY to FULL
    IF table_replident <> 'f' AND has_pkey IS NULL AND table_persistence = 'p' THEN
        EXECUTE format('ALTER TABLE %s.%s REPLICA IDENTITY %s', quote_ident(schema_name), quote_ident(table_relname), 'FULL');
    END IF;

    -- Form the JSON containing the table information including column details, partition info etc
    table_info = json_build_object(
        'table_name', table_relname,
        'table_namespace_id', table_namespace_id,
        'partition_bound', partition_bound,
        'partition_key', partition_key,
        'partition_data', partition_data,
        'parent_table_id', parent_table_id::bigint,
        'columns', json_columns,
        'has_pkey', has_pkey,
        'table_persistence', table_persistence,
        'table_replident', table_replident,
        'rel_kind', rel_kind
    );

    -- RAISE NOTICE 'springtail: % op, %', command_tag, table_info::text;

    RETURN table_info;
END;
$$;

CREATE OR REPLACE FUNCTION __pg_springtail_triggers.springtail_event_trigger_for_table_ddl()
        RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj record;
    ind_obj record;
    msg text;
    index_columns json;
    table_info json;
    command_tag text;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() AS cmd
        WHERE cmd.command_tag IN ('ALTER TABLE', 'CREATE TABLE', 'ALTER INDEX')
    LOOP
        -- RAISE NOTICE 'springtail: % op, %, %, %', obj.command_tag, obj.object_identity, obj.objid, obj.object_type;
        -- IF obj.command_tag NOT IN ('ALTER TABLE', 'CREATE TABLE') THEN
        --     CONTINUE;
        -- END IF;
        IF obj.object_type <> 'table' THEN
            CONTINUE;
        END IF;

        -- Handle table events
        SELECT __pg_springtail_triggers.springtail_handle_table_events(obj.objid, obj.object_identity, obj.command_tag, obj.schema_name) INTO table_info;

        -- Check if the JSON is empty
        IF table_info IS NULL THEN
            CONTINUE;
        END IF;

        IF table_info->>'table_persistence' <> 'p' THEN
            --- RAISE NOTICE 'springtail: skipping operation %, on object %, with identity %, due to wrong persistence type: %', obj.command_tag, obj.object_type, obj.object_identity, table_persistence;
            CONTINUE;
        END IF;

        IF obj.command_tag = 'ALTER INDEX' THEN
            command_tag := 'ALTER TABLE';
        ELSE
            command_tag := obj.command_tag;
        END IF;

        -- Note: obj.object_name is not available
        msg := json_build_object('xid', txid_current(),
            'cmd', command_tag,
            'oid', obj.objid::bigint,
            'obj', obj.object_type,
            'schema', obj.schema_name,
            'table', table_info->'table_name',
            'columns', table_info->'columns',
            'parent_table_id', table_info->'parent_table_id',
            'partition_bound', table_info->'partition_bound',
            'partition_key', table_info->'partition_key',
            'partition_data', table_info->'partition_data'
        );

        -- command_tag is CREATE TABLE or ALTER TABLE
        PERFORM pg_logical_emit_message(true, 'springtail:' || command_tag, msg::text);
        -- RAISE NOTICE 'springtail: % op, %', command_tag, msg::text;

        -- If a table is altered, and it has a primary key, set REPLICA IDENTITY to DEFAULT
        IF obj.command_tag IN ('ALTER TABLE', 'ALTER INDEX') AND table_info->>'table_replident' = 'f' AND table_info->>'has_pkey' = 'true' THEN
            EXECUTE format('ALTER TABLE %s.%s REPLICA IDENTITY %s', quote_ident(obj.schema_name), quote_ident(table_info->>'table_name'), 'DEFAULT');
        END IF;

        -- XXX To fix for ALTER TABLE later; right now indexes dropped or alters in alter table are not modified
        IF obj.command_tag = 'CREATE TABLE' THEN
            -- Runs a query to get the list of indexes on a table
            -- Only retrieve the secondary indexes
            -- NOTE: this is very similar to the springtail_generate_index_message function
            -- but we don't have the index oids here so we need to get them.
            FOR ind_obj IN SELECT
                    quote_ident(n.nspname) || '.' || quote_ident(ci.relname) AS index_identity,
                    n.nspname AS schema_name,
                    i.indexrelid AS index_oid,
                    c.oid AS table_oid,
                    c.relname AS table_name,
                    i.indisunique AS is_unique,
                    i.indisprimary AS primary_idx,
                    i.indkey AS indkey
                FROM pg_index i
                JOIN pg_class c ON c.oid = i.indexrelid
                JOIN pg_class ci ON ci.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE i.indisprimary IS FALSE
                AND c.oid IN (SELECT indexrelid FROM pg_index WHERE indrelid = obj.objid)
            LOOP
                -- get index columns
                SELECT json_agg(json_col)
                FROM (
                    SELECT json_build_object('name', pga.attname,
                        'position', pga.attnum,
                        'idx_position', array_position(ind_obj.indkey, pga.attnum)
                    ) AS json_col
                    FROM pg_attribute pga
                    WHERE
                        pga.attrelid=obj.objid
                        AND (array_position(ind_obj.indkey, pga.attnum) IS NOT NULL)
                        AND attisdropped=false
                ) AS obj_select
                INTO index_columns;

                -- Build the replication message object
                msg := json_build_object(
                    'xid', txid_current(),
                    'cmd', 'CREATE INDEX',
                    'oid', ind_obj.index_oid::bigint,
                    'obj', 'index',
                    'schema', ind_obj.schema_name,
                    'identity', ind_obj.index_identity,
                    'table_oid', ind_obj.table_oid::bigint,
                    'table_name', ind_obj.table_name,
                    'is_unique', ind_obj.is_unique,
                    'columns', index_columns
                );

                -- command_tag is CREATE INDEX or ALTER INDEX ( XXX figure out the cmd tag for alter )
                PERFORM pg_logical_emit_message(true, 'springtail:' || 'CREATE INDEX', msg::text);
            END LOOP;
        END IF;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION __pg_springtail_triggers.springtail_event_trigger_for_index_ddl()
        RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    ind_obj record;
    json_columns json;
    obj record;
    msg text;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() as cmd
        WHERE cmd.object_type = 'index'
    LOOP
        PERFORM __pg_springtail_triggers.springtail_handle_index_events(obj.objid, obj.object_identity, obj.object_type, obj.command_tag, obj.schema_name);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION __pg_springtail_triggers.springtail_event_trigger_for_schema_ddl()
        RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj record;
    schema_obj record;
    msg text;
    table_info json;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() as cmd
    LOOP
        msg := NULL;

        IF obj.command_tag IN ('CREATE SCHEMA', 'ALTER SCHEMA') THEN
            -- command_tag is CREATE SCHEMA or ALTER SCHEMA
            -- RAISE NOTICE 'springtail springtail_event_trigger_for_schema_ddl: obj.command_tag %, obj.objid %, obj.type %, obj.object_identity %',
            --     obj.command_tag, obj.objid, obj.object_type, obj.object_identity;

            SELECT nsp.oid AS schema_oid,
                nsp.nspname AS schema_name
            INTO schema_obj
            FROM pg_catalog.pg_namespace nsp
            WHERE nsp.oid = obj.objid;

            msg := json_build_object('xid', txid_current(),
                'cmd', obj.command_tag,
                'oid', schema_obj.schema_oid::bigint,
                'obj', obj.object_type,
                'name', schema_obj.schema_name);

        ELSIF obj.command_tag = 'CREATE TABLE' THEN
            -- command_tag is CREATE TABLE inside CREATE SCHEMA
            -- Handle table events
            SELECT __pg_springtail_triggers.springtail_handle_table_events(obj.objid, obj.object_identity, obj.command_tag, obj.schema_name) INTO table_info;

            -- Check if the JSON is empty
            IF table_info IS NULL THEN
                CONTINUE;
            END IF;

            msg := json_build_object('xid', txid_current(),
                'cmd', obj.command_tag,
                'oid', obj.objid::bigint,
                'obj', obj.object_type,
                'schema', obj.schema_name,
                'table', table_info->'table_name',
                'columns', table_info->'columns',
                'parent_table_id', table_info->'parent_table_id',
                'partition_bound', table_info->'partition_bound',
                'partition_key', table_info->'partition_key',
                'partition_data', table_info->'partition_data'
            );

        ELSIF obj.command_tag = 'CREATE INDEX' THEN
            PERFORM __pg_springtail_triggers.springtail_handle_index_events(obj.objid, obj.object_identity, obj.object_type, obj.command_tag, obj.schema_name);
            CONTINUE;
        END IF;

        IF msg IS NOT NULL THEN
            PERFORM pg_logical_emit_message(true, 'springtail:' || obj.command_tag, msg::text);
        END IF;

    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION __pg_springtail_triggers.springtail_event_trigger_for_types_ddl()
        RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj record;
    enum_obj record;
    msg text;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() as cmd
    LOOP
        SELECT t.oid::integer AS enum_type_oid,
               n.oid::integer AS namespace_oid,
               n.nspname::text AS schema,
               t.typname::text AS enum_type_name,
               json_agg(json_build_object(e.enumlabel::text, e.enumsortorder::real) ORDER BY e.enumsortorder)::text AS value
        FROM pg_enum e
        JOIN pg_type t ON t.oid = e.enumtypid
        JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE t.oid = obj.objid AND typcategory = 'E'
        GROUP BY t.oid, n.oid, n.nspname, t.typname
        INTO enum_obj;

        IF (enum_obj IS NULL) THEN
            CONTINUE;
        END IF;

        msg := json_build_object('xid', txid_current(),
            'oid', enum_obj.enum_type_oid,
            'type', 'E',
            'ns_oid', enum_obj.namespace_oid,
            'schema', enum_obj.schema,
            'name', enum_obj.enum_type_name,
            'value', enum_obj.value);

        -- RAISE NOTICE 'springtail: %', msg::text;

        PERFORM pg_logical_emit_message(true, 'springtail:' || obj.command_tag, msg::text);

    END LOOP;
END;
$$;

DROP EVENT TRIGGER IF EXISTS springtail_event_trigger_for_drops;
CREATE EVENT TRIGGER springtail_event_trigger_for_drops
   ON sql_drop
   WHEN TAG IN ( 'DROP TABLE', 'DROP INDEX', 'DROP SCHEMA', 'DROP TYPE' )
   EXECUTE FUNCTION __pg_springtail_triggers.springtail_event_trigger_for_drops();

DROP EVENT TRIGGER IF EXISTS springtail_event_trigger_for_table_ddl;
CREATE EVENT TRIGGER springtail_event_trigger_for_table_ddl
   ON ddl_command_end
   WHEN TAG IN ( 'CREATE TABLE', 'ALTER TABLE', 'ALTER INDEX' )
   EXECUTE FUNCTION __pg_springtail_triggers.springtail_event_trigger_for_table_ddl();

DROP EVENT TRIGGER IF EXISTS springtail_event_trigger_for_schema_ddl;
CREATE EVENT TRIGGER springtail_event_trigger_for_schema_ddl
   ON ddl_command_end
   WHEN TAG IN ( 'CREATE SCHEMA', 'ALTER SCHEMA' )
   EXECUTE FUNCTION __pg_springtail_triggers.springtail_event_trigger_for_schema_ddl();

DROP EVENT TRIGGER IF EXISTS springtail_event_trigger_for_index_ddl;
CREATE EVENT TRIGGER springtail_event_trigger_for_index_ddl
   ON ddl_command_end
   WHEN TAG IN ( 'CREATE INDEX' )
   EXECUTE FUNCTION __pg_springtail_triggers.springtail_event_trigger_for_index_ddl();

DROP EVENT TRIGGER IF EXISTS springtail_event_trigger_for_types_ddl;
CREATE EVENT TRIGGER springtail_event_trigger_for_types_ddl
   ON ddl_command_end
   WHEN TAG IN ( 'CREATE TYPE', 'ALTER TYPE' )
   EXECUTE FUNCTION __pg_springtail_triggers.springtail_event_trigger_for_types_ddl();

-- Select all users and their databases with access to springtail
-- If springtail_user role exists, only users with that role are returned
-- otherwise all users are returned
CREATE OR REPLACE FUNCTION __pg_springtail_triggers.springtail_get_user_access()
    RETURNS TABLE (username text, password text, databases text)
    LANGUAGE plpgsql
    SECURITY DEFINER AS $$
DECLARE
    user_record record;
    db_record record;
    db_list json;
    has_springtail_role boolean;
BEGIN
    -- Check if springtail_user role exists
    SELECT
        EXISTS (
            SELECT 1
            FROM pg_roles
            WHERE rolname = 'springtail_user'
        )
    INTO has_springtail_role;

    -- If springtail_user role exists, only users with that role are returned
    IF has_springtail_role THEN
        RETURN QUERY SELECT
            s.usename::text AS username,
            s.passwd AS password,
            json_agg(d.datname ORDER BY d.datname)::text AS databases
        FROM pg_shadow s
        JOIN pg_roles r ON s.usesysid = r.oid
        CROSS JOIN pg_database d
        WHERE (s.valuntil IS NULL OR s.valuntil > now())
          AND s.passwd IS NOT NULL
          AND (s.passwd ilike 'MD5%' OR s.passwd ilike 'SCRAM%')
          AND r.rolcanlogin IS TRUE
          AND has_database_privilege(s.usename, d.datname, 'CONNECT')
          AND EXISTS ( SELECT 1
            FROM pg_auth_members m
            JOIN pg_roles r ON m.roleid = r.oid
            WHERE m.member = s.usesysid
            AND r.rolname = 'springtail_user' )
        GROUP BY s.usename, s.passwd
        ORDER BY s.usename;
    ELSE
        -- If springtail_user role does not exist, all users are returned
        RETURN QUERY SELECT
            s.usename::text AS username,
            s.passwd AS password,
            json_agg(d.datname ORDER BY d.datname)::text AS databases
        FROM pg_shadow s
        JOIN pg_roles r ON s.usesysid = r.oid
        CROSS JOIN pg_database d
        WHERE (s.valuntil IS NULL OR s.valuntil > now())
          AND s.passwd IS NOT NULL
          AND (s.passwd ilike 'MD5%' OR s.passwd ilike 'SCRAM%')
          AND r.rolcanlogin IS TRUE
          AND has_database_privilege(s.usename, d.datname, 'CONNECT')
        GROUP BY s.usename, s.passwd
        ORDER BY s.usename;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION __pg_springtail_triggers.set_identity_on_tables_without_pk()
RETURNS void AS $$
DECLARE
    tbl RECORD;
    has_pk BOOLEAN;
BEGIN
    FOR tbl IN
        -- find all regular tables that aren't part of the metadata schemas
        SELECT relname::text AS tablename,
               nspname::text AS schemaname,
               pg_class.oid::integer AS oid
        FROM pg_catalog.pg_class
        JOIN pg_catalog.pg_namespace
        ON relnamespace=pg_namespace.oid
        WHERE relkind = 'r'
        AND nspname NOT LIKE 'pg_%'
        AND nspname != 'information_schema'
        ORDER BY pg_class.oid
    LOOP
        -- check if table has a primary key
        SELECT EXISTS (
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = tbl.oid
              AND contype = 'p'
        ) INTO has_pk;

        IF NOT has_pk THEN
            EXECUTE format(
                'ALTER TABLE %I.%I REPLICA IDENTITY FULL;',
                tbl.schemaname, tbl.tablename
            );
            RAISE NOTICE 'Set REPLICA IDENTITY FULL on %.%', tbl.schemaname, tbl.tablename;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
