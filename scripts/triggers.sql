-- Triggers for create/alter table and drop table events
-- https://www.postgresql.org/docs/current/plpgsql-trigger.html

CREATE OR REPLACE FUNCTION springtail_event_trigger_for_drops()
        RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj record;
    msg json;
    items record;
    tag_name text;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        -- Check for table or index drops
        IF NOT obj.is_temporary AND (obj.object_type = 'table' OR obj.object_type = 'index') THEN

            -- sometimes tg_tag is DROP TABLE even if type is index
            IF obj.object_type = 'table' THEN
                tag_name := 'DROP TABLE';
            ELSE
                tag_name := 'DROP INDEX';
            END IF;

            -- generate message same for DROP TABLE/INDEX
            msg := json_build_object('cmd', tag_name,
                'oid', obj.objid::bigint, -- oid is unsigned int, but comes as string
                'obj', obj.object_type,
                'schema', obj.schema_name,
                'name', obj.object_name,
                'identity', obj.object_identity);

            --- RAISE NOTICE 'springtail: % op, %.%', tag_name, obj.schema_name, obj.object_name;

            -- tag_name is DROP TABLE or DROP INDEX
            PERFORM pg_logical_emit_message(true, 'springtail:' || tag_name, msg::text);
        END IF;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION springtail_event_trigger_for_table_ddl()
        RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj record;
    msg text;
    json_columns json;
    has_pkey boolean;
    table_persistence "char";
    table_replident "char";
    table_relname text;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() AS cmd
        WHERE cmd.command_tag IN ('ALTER TABLE', 'CREATE TABLE')
    LOOP
        SELECT relname, relreplident, relpersistence
        FROM pg_class
        WHERE oid = obj.objid
        INTO table_relname, table_replident, table_persistence;

        IF table_persistence <> 'p' THEN
            --- RAISE NOTICE 'springtail: skipping operation %, on object %, with identity %, due to wrong persistence type: %', obj.command_tag, obj.object_type, obj.object_identity, table_persistence;
            RETURN;
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
                'is_generated', (is_generated <> 'NEVER')
            ) AS json_col
            FROM pg_attribute pga
            JOIN information_schema.columns
            ON column_name=pga.attname
            LEFT OUTER JOIN pg_index pgi
            ON pga.attrelid=pgi.indrelid AND pgi.indisprimary
            WHERE pga.attrelid=obj.objid
              AND quote_literal(table_schema) = quote_literal(obj.schema_name)
              AND quote_literal(table_name) = quote_literal(table_relname)
              AND atttypid > 0
            ORDER BY ordinal_position
        ) AS obj_select
        INTO json_columns;

        -- Note: obj.object_name is not available
        msg := json_build_object('xid', txid_current(),
            'cmd', obj.command_tag,
            'oid', obj.objid::bigint,
            'obj', obj.object_type,
            'schema', obj.schema_name,
            'table', table_relname,
            'columns', json_columns);

        -- command_tag is CREATE TABLE or ALTER TABLE
        PERFORM pg_logical_emit_message(true, 'springtail:' || obj.command_tag, msg::text);

        -- RAISE NOTICE 'springtail: % op, %, %, %', obj.command_tag, obj.object_identity, obj.objid, table_replident;

        SELECT true WHERE json_columns::jsonb @> '[{"is_pkey": true}]'::jsonb INTO has_pkey;

        -- If a table is created or altered, and it doesn't have a primary key, set REPLICA IDENTITY to FULL
        IF table_replident <> 'f' AND has_pkey IS NULL THEN
            PERFORM springtail_set_replica_identity(obj.schema_name, table_relname, true);
        END IF;

        -- If a table is altered, and it has a primary key, set REPLICA IDENTITY to DEFAULT
        IF table_replident = 'f' AND has_pkey IS TRUE THEN
            PERFORM springtail_set_replica_identity(obj.schema_namem, table_relname, false);
        END IF;

    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION springtail_set_replica_identity(schema_name text, table_name text, full_ident boolean DEFAULT true)
        RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    ident text;
BEGIN
    ident := quote_ident(schema_name) || '.' || quote_ident(table_name);
    IF full_ident THEN
        --- RAISE NOTICE 'springtail: setting REPLICA IDENTITY FULL for %', identity;
        EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', ident);
    ELSE
        --- RAISE NOTICE 'springtail: setting REPLICA IDENTITY DEFAULT for %', identity;
        EXECUTE format('ALTER TABLE %s REPLICA IDENTITY DEFAULT', ident);
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION springtail_event_trigger_for_index_ddl()
        RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj record;
    ind_obj record;
    msg text;
    json_columns json;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() as cmd
        WHERE cmd.object_type = 'index'
    LOOP
        -- additionl index and table info
        EXECUTE format('SELECT
                c.oid AS table_oid,
                c.relname AS table_name,
                i.indisunique AS is_unique,
                i.indisprimary AS primary_idx,
                i.indkey AS indkey
            FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
            WHERE i.indexrelid = %s', obj.objid) INTO ind_obj;

        IF ind_obj.primary_idx is true THEN
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
            'cmd', obj.command_tag,
            'oid', obj.objid::bigint,
            'obj', obj.object_type,
            'schema', obj.schema_name,
            'identity', obj.object_identity,
            'table_oid', ind_obj.table_oid::bigint,
            'table_name', ind_obj.table_name,
            'is_unique', ind_obj.is_unique,
            'columns', json_columns);

        -- command_tag is CREATE TABLE or ALTER TABLE
        PERFORM pg_logical_emit_message(true, 'springtail:' || obj.command_tag, msg::text);

        -- RAISE NOTICE 'springtail: % op, %, %, %', obj.command_tag, obj.object_identity, obj.objsubid, tab_obj.primary_idx;
    END LOOP;
END;
$$;

DROP EVENT TRIGGER IF EXISTS springtail_event_trigger_for_drops;
CREATE EVENT TRIGGER springtail_event_trigger_for_drops
   ON sql_drop
   WHEN TAG IN ( 'DROP TABLE', 'DROP INDEX')
   EXECUTE FUNCTION springtail_event_trigger_for_drops();

DROP EVENT TRIGGER IF EXISTS springtail_event_trigger_for_table_ddl;
CREATE EVENT TRIGGER springtail_event_trigger_for_table_ddl
   ON ddl_command_end
   WHEN TAG IN ( 'CREATE TABLE', 'ALTER TABLE', 'CREATE SCHEMA' )
   EXECUTE FUNCTION springtail_event_trigger_for_table_ddl();

DROP EVENT TRIGGER IF EXISTS springtail_event_trigger_for_index_ddl;
CREATE EVENT TRIGGER springtail_event_trigger_for_index_ddl
   ON ddl_command_end
   WHEN TAG IN ( 'CREATE INDEX' )
   EXECUTE FUNCTION springtail_event_trigger_for_index_ddl();


-- Select all users and their databases with access to springtail
-- If springtail_user role exists, only users with that role are returned
-- otherwise all users are returned
CREATE OR REPLACE FUNCTION springtail_get_user_access()
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

-- Clean up function to drop the other functions and triggers
CREATE OR REPLACE FUNCTION springtail_cleanup()
    RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    -- Drop event functions
    DROP FUNCTION IF EXISTS springtail_event_trigger_for_drops() CASCADE;
    DROP FUNCTION IF EXISTS springtail_event_trigger_for_table_ddl() CASCADE;
    DROP FUNCTION IF EXISTS springtail_set_replica_identity(identity regclass, full_ident boolean) CASCADE;
    DROP FUNCTION IF EXISTS springtail_event_trigger_for_index_ddl() CASCADE;
    DROP FUNCTION IF EXISTS springtail_get_user_access() CASCADE;
    -- Drop event triggers
    DROP EVENT TRIGGER IF EXISTS springtail_event_trigger_for_drops;
    DROP EVENT TRIGGER IF EXISTS springtail_event_trigger_for_table_ddl;
    DROP EVENT TRIGGER IF EXISTS springtail_event_trigger_for_index_ddl;
END;
$$;
