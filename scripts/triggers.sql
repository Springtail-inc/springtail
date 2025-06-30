-- Triggers for create/alter table and drop table events
-- https://www.postgresql.org/docs/current/plpgsql-trigger.html

CREATE SCHEMA IF NOT EXISTS __pg_springtail_triggers; -- NOTE: if this schema changes, change pg_copy_table.cc too

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

CREATE OR REPLACE FUNCTION __pg_springtail_triggers.springtail_event_trigger_for_table_ddl()
        RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj record;
    ind_obj record;
    msg text;
    json_columns json;
    index_columns json;
    has_pkey boolean;
    table_persistence "char";
    table_replident "char";
    rel_kind "char";
    table_relname text;
    table_info RECORD;
    command_tag text;
    rel_rowsecurity boolean;
    rel_forcerowsecurity boolean;
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

        -- BEGIN of what should have been a function, if you change it here,
        -- it should also be change in the springtail_event_trigger_for_schema_ddl()
        SELECT relname, relreplident, relpersistence, relkind, relrowsecurity, relforcerowsecurity
        FROM pg_class
        WHERE oid = obj.objid
        INTO table_relname, table_replident, table_persistence, rel_kind, rel_rowsecurity, rel_forcerowsecurity;

        -- This is a corner case when an index is renamed through "ALTER TABLE" statement
        -- In this case our object is an index, not a table. So, we can't do anything with it here.
        -- 'i' - normal index, 'I' - partitioned index
        -- 'r' - normal table, 'p' - partitioned table
        IF rel_kind <> 'r' AND rel_kind <> 'p' THEN
            CONTINUE;
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
            WHERE pga.attrelid=obj.objid
                AND quote_literal(table_schema) = quote_literal(obj.schema_name)
                AND quote_literal(table_name) = quote_literal(table_relname)
                AND atttypid > 0
            ORDER BY ordinal_position
        ) AS obj_select
        INTO json_columns;

        SELECT true WHERE json_columns::jsonb @> '[{"is_pkey": true}]'::jsonb INTO has_pkey;

        -- If a table is created or altered, and it doesn't have a primary key, set REPLICA IDENTITY to FULL
        IF table_replident <> 'f' AND has_pkey IS NULL AND table_persistence = 'p' THEN
            EXECUTE format('ALTER TABLE %s.%s REPLICA IDENTITY %s', quote_ident(obj.schema_name), quote_ident(table_relname), 'FULL');
        END IF;
        -- END of what should have been a function, if you change it here,
        -- it should also be change in the springtail_event_trigger_for_schema_ddl()

        IF table_persistence <> 'p' THEN
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
            'table', table_relname,
            'columns', json_columns,
            'rls_enabled', rel_rowsecurity,
            'rls_forced', rel_forcerowsecurity);

        -- command_tag is CREATE TABLE or ALTER TABLE
        PERFORM pg_logical_emit_message(true, 'springtail:' || command_tag, msg::text);

        -- RAISE NOTICE 'springtail: % op, %, %, %', obj.command_tag, obj.object_identity, obj.objid, table_replident;

        -- If a table is altered, and it has a primary key, set REPLICA IDENTITY to DEFAULT
        IF obj.command_tag IN ('ALTER TABLE', 'ALTER INDEX') AND table_replident = 'f' AND has_pkey IS TRUE THEN
            EXECUTE format('ALTER TABLE %s.%s REPLICA IDENTITY %s', quote_ident(obj.schema_name), quote_ident(table_relname), 'DEFAULT');
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

        -- RAISE NOTICE 'springtail: % op, %, %, %', obj.command_tag, obj.object_type, obj.object_identity, obj.objid;

        -- BEGIN of what should have been a function, if you change it here,
        -- it should also be change in the springtail_event_trigger_for_schema_ddl()

        -- additionl index and table info
        EXECUTE format('SELECT
                c.oid AS table_oid,
                c.relname AS table_name,
                i.indisunique AS is_unique,
                i.indisprimary AS primary_idx,
                i.indkey AS indkey
            FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
            WHERE i.indexrelid = %s', obj.objid) INTO ind_obj;

        IF ind_obj.primary_idx IS true THEN
            CONTINUE;
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

        -- END of what should have been a function, if you change it here,
        -- it should also be change in the springtail_event_trigger_for_table_ddl()

        -- command_tag is CREATE INDEX
        PERFORM pg_logical_emit_message(true, 'springtail:' || obj.command_tag, msg::text);

        -- RAISE NOTICE 'springtail: % op, %, %, %', obj.command_tag, obj.object_identity, obj.objsubid, tab_obj.primary_idx;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION __pg_springtail_triggers.springtail_event_trigger_for_schema_ddl()
        RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj record;
    schema_obj record;
    msg text;
    table_persistence "char";
    table_replident "char";
    table_relname text;
    rel_kind "char";
    has_pkey boolean;
    ind_obj record;
    json_columns json;
    table_info RECORD;
    rel_rowsecurity boolean;
    rel_forcerowsecurity boolean;
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
            -- RAISE NOTICE 'springtail springtail_event_trigger_for_schema_ddl: obj.command_tag %, obj.type %, obj.object_identity %, obj.objid %, obj.schema_name %',
            --     obj.command_tag, obj.object_type, obj.object_identity, obj.objid, obj.schema_name;

            -- BEGIN of what should have been a function, if you change it here,
            -- it should also be change in the springtail_event_trigger_for_table_ddl()
            SELECT relname, relreplident, relpersistence, relkind, relrowsecurity, relforcerowsecurity
            FROM pg_class
            WHERE oid = obj.objid
            INTO table_relname, table_replident, table_persistence, rel_kind, rel_rowsecurity, rel_forcerowsecurity;

            -- This is a corner case when an index is renamed through "ALTER TABLE" statement
            -- In this case our object is an index, not a table. So, we can't do anything with it here.
            -- 'i' - normal index, 'I' - partitioned index
            -- 'r' - normal table, 'p' - partitioned table
            IF rel_kind <> 'r' AND rel_kind <> 'p' THEN
                CONTINUE;
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
                WHERE pga.attrelid=obj.objid
                    AND quote_literal(table_schema) = quote_literal(obj.schema_name)
                    AND quote_literal(table_name) = quote_literal(table_relname)
                    AND atttypid > 0
                ORDER BY ordinal_position
            ) AS obj_select
            INTO json_columns;

            SELECT true WHERE json_columns::jsonb @> '[{"is_pkey": true}]'::jsonb INTO has_pkey;

            -- If a table is created or altered, and it doesn't have a primary key, set REPLICA IDENTITY to FULL
            IF table_replident <> 'f' AND has_pkey IS NULL AND table_persistence = 'p' THEN
                EXECUTE format('ALTER TABLE %s.%s REPLICA IDENTITY %s', quote_ident(obj.schema_name), quote_ident(table_relname), 'FULL');
            END IF;
            -- END of what should have been a function, if you change it here,
            -- it should also be change in the springtail_event_trigger_for_table_ddl()

            msg := json_build_object('xid', txid_current(),
                'cmd', obj.command_tag,
                'oid', obj.objid::bigint,
                'obj', obj.object_type,
                'schema', obj.schema_name,
                'table', table_relname,
                'columns', json_columns,
                'rls_enabled', rel_rowsecurity::boolean,
                'rls_forced', rel_forcerowsecurity::boolean
            );

        ELSIF obj.command_tag = 'CREATE INDEX' THEN
            -- command_tag is CREATE INDEX inside CREATE SCHEMA
            -- RAISE NOTICE 'springtail springtail_event_trigger_for_schema_ddl: obj.command_tag %, obj.type %, obj.object_identity %, obj.objid %, obj.schema_name %',
            --     obj.command_tag, obj.object_type, obj.object_identity, obj.objid, obj.schema_name;

            -- BEGIN of what should have been a function, if you change it here,
            -- it should also be change in the springtail_event_trigger_for_index_ddl()

            -- additionl index and table info
            EXECUTE format('SELECT
                    c.oid AS table_oid,
                    c.relname AS table_name,
                    i.indisunique AS is_unique,
                    i.indisprimary AS primary_idx,
                    i.indkey AS indkey
                FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
                WHERE i.indexrelid = %s', obj.objid) INTO ind_obj;

            IF ind_obj.primary_idx IS true THEN
                CONTINUE;
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

            -- END of what should have been a function, if you change it here,
            -- it should also be change in the springtail_event_trigger_for_table_ddl()

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
