-- springtail_fdw--1.0.sql
CREATE FUNCTION springtail_fdw_handler()
    RETURNS fdw_handler
    AS '$libdir/springtail_fdw', 'springtail_fdw_handler'
    LANGUAGE c STRICT;

CREATE FUNCTION springtail_fdw_validator(text[], oid)
    RETURNS void
    AS '$libdir/springtail_fdw', 'springtail_fdw_validator'
    LANGUAGE c STRICT;

CREATE FOREIGN DATA WRAPPER springtail_fdw
    HANDLER springtail_fdw_handler
    VALIDATOR springtail_fdw_validator;
