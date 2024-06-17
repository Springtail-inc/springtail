FROM springtail:base as debug_build

COPY ./ /springtail
WORKDIR /springtail

RUN ./clean.sh
RUN ./debug.sh -j 4

FROM ubuntu:jammy

RUN apt-get update && apt-get install -y \
    libdw-dev libelf-dev libbsd-dev \
    && apt-get clean autoclean \
    && apt-get autoremove --yes \
    && rm -rf /var/lib/{apt,cache,log,dpkg}

COPY --from=debug_build /springtail/debug/src/pg_log_mgr/pg_log_mgr_daemon /bin
COPY --from=debug_build /springtail/debug/src/xid_mgr/xid_mgr_daemon /bin
COPY --from=debug_build /springtail/debug/src/write_cache/write_cache_daemon /bin
COPY system.json /bin

CMD ["/bin/pg_log_mgr_daemon"]

