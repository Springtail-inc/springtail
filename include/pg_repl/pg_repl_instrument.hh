#pragma once

#include <common/logging.hh>

#define PROFILE_INGEST

#ifdef PROFILE_INGEST
#define PROFILE_INGEST_ENABLED
#define INSTRUMENT_INGEST_DATA(type, name) type name;
#define INSTRUMENT_INGEST(lvl, code_block) if (lvl >= springtail::logging::Logger::observability_level()) code_block
#else
#define INSTRUMENT_INGEST_DATA(type, name)
#define INSTRUMENT_INGEST(observability_level, code_block)
#endif

