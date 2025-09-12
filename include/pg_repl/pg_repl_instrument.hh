#pragma once

#ifdef PROFILE_INGEST
#define PROFILE_INGEST_ENABLED
#define INSTRUMENT_INGEST_DATA(type, name) type name;
#define INSTRUMENT_INGEST(code_block) code_block
#else
#define INSTRUMENT_INGEST_DATA(type, name)
#define INSTRUMENT_INGEST(code_block)
#endif

