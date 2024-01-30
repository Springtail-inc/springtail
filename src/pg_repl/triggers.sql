-- Triggers for create/alter table and drop table events
-- https://www.postgresql.org/docs/current/plpgsql-trigger.html

CREATE OR REPLACE FUNCTION springtail_event_trigger_for_drops()
        RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj record;
    msg json;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        IF NOT obj.is_temporary AND obj.object_type = 'table' THEN
            msg := json_build_object('cmd', tg_tag,
                'oid', obj.objid::bigint, -- oid is unsigned int, but comes as string
                'obj', obj.object_type,
                'schema', obj.schema_name,
                'name', obj.object_name,
                'identity', obj.object_identity);

            PERFORM pg_logical_emit_message(true, 'springtail ' || tg_tag, msg::text);
        END IF;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION springtail_event_trigger_for_ddl()
        RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    obj record;
    msg text;
    json_columns json;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
        SELECT json_agg(json_col)
        FROM (
            SELECT json_build_object('name', column_name,
                'is_nullable', is_nullable::boolean,
                'type', udt_name, 
                'default', column_default,
                'is_pkey', coalesce((pga.attnum=any(pgi.indkey))::boolean, false),
                'position', ordinal_position
            ) AS json_col
            FROM pg_attribute pga
            JOIN information_schema.columns
            ON column_name=pga.attname
            LEFT OUTER JOIN pg_index pgi
            ON pga.attrelid=pgi.indrelid
            WHERE pga.attrelid=obj.objid
              AND table_schema || '.' || table_name = obj.object_identity
            ORDER BY ordinal_position
        ) AS obj_select
        INTO json_columns;

        msg := json_build_object('xid', txid_current(),
            'cmd', obj.command_tag,
            'oid', obj.objid::bigint,
            'obj', obj.object_type,
            'schema', obj.schema_name,
            'columns', json_columns,
            'identity', obj.object_identity)::text;

        PERFORM pg_logical_emit_message(true, 'springtail ' || obj.command_tag, msg);
    END LOOP;
END;
$$;

DROP EVENT TRIGGER IF EXISTS springtail_event_trigger_for_drops;
CREATE EVENT TRIGGER springtail_event_trigger_for_drops
   ON sql_drop
   WHEN TAG IN ( 'DROP TABLE' )
   EXECUTE FUNCTION springtail_event_trigger_for_drops();

DROP EVENT TRIGGER IF EXISTS springtail_event_trigger_for_ddl;
CREATE EVENT TRIGGER springtail_event_trigger_for_ddl
   ON ddl_command_end 
   WHEN TAG IN ( 'CREATE TABLE', 'ALTER TABLE' )
   EXECUTE FUNCTION springtail_event_trigger_for_ddl();