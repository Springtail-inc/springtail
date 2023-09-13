# Springtail

## Pre-requisites:
- libpqxx: libpqxx via homebrew on Mac: brew install libpqxx
- libpq: libpq is installed by homebrew with libpqxx, however it may need to be symlinked
  - /opt/homebrew/lib/libpq.5.15.dylib -> ../Cellar/libpq/15.4/lib/libpq.5.15.dylib
  - /opt/homebrew/lib/libpq.a -> ../Cellar/libpq/15.4/lib/libpq.a
  - /opt/homebrew/lib/libpq.dylib -> ../Cellar/libpq/15.4/lib/libpq.dylib
