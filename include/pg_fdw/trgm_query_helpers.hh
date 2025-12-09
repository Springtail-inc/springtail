#pragma once

extern "C" {
#include <postgres.h>
#include "fmgr.h"

#include "access/gin.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_opclass.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "nodes/makefuncs.h"
#include "commands/defrem.h"
#include "utils/pg_locale.h"
}

#include <vector>
#include <string>

/**
 * extract_gin_keys_from_string
 *
 * @param value         Input string to extract GIN keys from.
 * @param opclass_name  Name of the GIN opclass (e.g. "gin_trgm_ops").
 * @param collation     Collation OID to use for extraction.
 *
 * @return Vector of keys produced by the opclass’s extractQuery function.
 */
std::vector<std::string>
extract_gin_keys_from_string(const std::string &value,
                             const std::string &opclass_name,
                             Oid collation,
                             int op_strategy_number);

std::string unpack_trigram_int_to_string(uint32_t v);
