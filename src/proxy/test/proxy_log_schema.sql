create database proxy_logs;
\connect proxy_logs;

create table if not exists sessions (
    id serial8 primary key,
    server_id bigint not null,
    session_id bigint not null,
    type text not null,
    customer_id bigint not null,
    endpoint text,
    created_at bigint not null,
    disconnected_at bigint
);

create table if not exists messages (
    id serial8 primary key,
    sessions_id_pkey bigint not null,
    server_id bigint not null,
    session_id bigint not null,
    type text not null,
    client_session_id bigint not null,
    seq_num bigint not null,
    code text not null,
    message text not null,
    data_row_hash text,
    timestamp bigint
);