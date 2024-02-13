FROM ubuntu:jammy

RUN apt-get update && apt-get install -y build-essential cmake autoconf bison pkg-config flex libdw-dev libdwarf-dev binutils-dev clang git curl zip unzip tar ninja-build

COPY ./ /springtail

WORKDIR /springtail

ENV ZIC=true
ENV VCPKG_FORCE_SYSTEM_BINARIES=1

RUN ./vcpkg.sh

RUN ./clean.sh

RUN ./debug.sh -j 2
