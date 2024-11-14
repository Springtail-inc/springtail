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

            RAISE NOTICE 'springtail: % op, %.%', tag_name, obj.schema_name, obj.object_name;

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
            RAISE NOTICE 'springtail: skipping operation %, on object %, with identity %, due to wrong persistence type: %', obj.command_tag, obj.object_type, obj.object_identity, table_persistence;
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

        RAISE NOTICE 'springtail: % op, %, %, %', obj.command_tag, obj.object_identity, obj.objid, table_replident;

        SELECT true WHERE json_columns::jsonb @> '[{"is_pkey": true}]'::jsonb INTO has_pkey;

        -- If a table is created or altered, and it doesn't have a primary key, set REPLICA IDENTITY to FULL
        IF table_replident <> 'f' AND has_pkey IS NULL THEN
            PERFORM springtail_set_replica_identity(obj.object_identity, true);
        END IF;

        -- If a table is altered, and it has a primary key, set REPLICA IDENTITY to DEFAULT
        IF table_replident = 'f' AND has_pkey IS TRUE THEN
            PERFORM springtail_set_replica_identity(obj.object_identity, false);
        END IF;

    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION springtail_set_replica_identity(identity regclass, full_ident boolean DEFAULT true)
        RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF full_ident THEN
        RAISE NOTICE 'springtail: setting REPLICA IDENTITY FULL for %', identity;
        EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', identity);
    ELSE
        RAISE NOTICE 'springtail: setting REPLICA IDENTITY DEFAULT for %', identity;
        EXECUTE format('ALTER TABLE %s REPLICA IDENTITY DEFAULT', identity);
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION springtail_event_trigger_for_index_ddl()
        RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj record;
    tab_obj record;
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
                i.indisprimary AS primary_idx
            FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
            WHERE i.indexrelid = %s', obj.objid) INTO tab_obj;

        IF tab_obj.primary_idx is true THEN
            RETURN;
        END IF;

        -- get index columns
        SELECT json_agg(json_col)
        FROM (
            SELECT json_build_object('name', column_name,
                'position', ordinal_position,
                'idx_position', array_position(pgi.indkey, pga.attnum)
            ) AS json_col
            FROM pg_attribute pga
            JOIN information_schema.columns
                ON column_name=pga.attname
            LEFT OUTER JOIN pg_index pgi
                ON pgi.indexrelid=obj.objid
            WHERE pgi.indexrelid=obj.objid
                AND pga.attrelid=pgi.indrelid
                AND obj.object_type = 'index'
                AND (array_position(pgi.indkey, pga.attnum) IS NOT NULL)
        ) AS obj_select
        INTO json_columns;

        -- build msg json object
        msg := json_build_object('xid', txid_current(),
            'cmd', obj.command_tag,
            'oid', obj.objid::bigint,
            'obj', obj.object_type,
            'schema', obj.schema_name,
            'identity', obj.object_identity,
            'table_oid', tab_obj.table_oid::bigint,
            'table_name', tab_obj.table_name,
            'is_unique', tab_obj.is_unique,
            'columns', json_columns);

        -- command_tag is CREATE TABLE or ALTER TABLE
        PERFORM pg_logical_emit_message(true, 'springtail:' || obj.command_tag, msg::text);

        RAISE NOTICE 'springtail: % op, %, %, %', obj.command_tag, obj.object_identity, obj.objsubid, tab_obj.primary_idx;
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




