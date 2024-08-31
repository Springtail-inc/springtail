-- create the replica DB
CREATE DATABASE replica_springtail;
\c replica_springtail

-- setup the FDW
CREATE EXTENSION springtail_fdw;
CREATE SERVER springtail_fdw_server FOREIGN DATA WRAPPER springtail_fdw;

-- import the remote table metadata
IMPORT FOREIGN SCHEMA public FROM SERVER springtail_fdw_server INTO public;
