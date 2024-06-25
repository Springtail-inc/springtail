-- create the replica DB
CREATE DATABASE springtail_replica;
\c springtail_replica

-- setup the FDW
CREATE EXTENSION springtail_fdw;
CREATE SERVER springtail_fdw_server FOREIGN DATA WRAPPER springtail_fdw;

-- import the remote table metadata
IMPORT FOREIGN SCHEMA public FROM SERVER springtail_fdw_server INTO public;
