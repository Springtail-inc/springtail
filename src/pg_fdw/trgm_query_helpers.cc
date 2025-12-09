#include <pg_fdw/trgm_query_helpers.hh>
extern "C" {
#include "postgres.h"
#include "fmgr.h"

#include "access/gin.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_opclass.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "nodes/makefuncs.h"
}

std::vector<std::string>
extract_gin_keys_from_string(const std::string &value,
        const std::string &opclass_name,
        Oid collation,
        int op_strategy_number)
{
    /* ---------------------------------------------------------
     * 1. Convert C++ string → PostgreSQL text*
     * --------------------------------------------------------- */
    text* input = cstring_to_text_with_len(value.data(), value.size());
    Datum queryDatum = PointerGetDatum(input);

    /* ---------------------------------------------------------
     * 2. Build PG name list for opclass lookup
     *    Example: "gin_trgm_ops" → ['gin_trgm_ops']
     * --------------------------------------------------------- */
    List *nameList = list_make1(makeString(pstrdup(opclass_name.c_str())));

    /* ---------------------------------------------------------
     * 3. Find the opclass OID for this name under the GIN AM
     * --------------------------------------------------------- */
    Oid opclassOid =
        get_opclass_oid(GIN_AM_OID,   // AM = GIN
                        nameList,
                        false);       // error if not found

    list_free_deep(nameList);

    /* ---------------------------------------------------------
     * 4. Get opfamily + input type for this opclass
     * --------------------------------------------------------- */
    Oid opfamily  = get_opclass_family(opclassOid);
    Oid inputType = get_opclass_input_type(opclassOid);

    /* ---------------------------------------------------------
     * 5. Find the extractQuery support function in pg_amproc
     *    (GIN support function #2)
     * --------------------------------------------------------- */
    Oid procOid = get_opfamily_proc(opfamily,
                                    inputType,
                                    inputType,
                                    GIN_EXTRACTQUERY_PROC);

    if (!OidIsValid(procOid))
        elog(ERROR, "no GIN extractQuery proc for opclass %s",
             opclass_name.c_str());

    /* ---------------------------------------------------------
     * 6. Build function descriptor to call the opclass function
     * --------------------------------------------------------- */
    FmgrInfo flinfo;
    fmgr_info(procOid, &flinfo);

    /* ---------------------------------------------------------
     * 7. Prepare OUT parameters for extractQuery
     * --------------------------------------------------------- */
    int32  nkeys = 0;
    bool  *partial_matches = nullptr;
    Pointer *extra_data    = nullptr;
    bool  *nullFlags       = nullptr;
    int32  searchMode      = GIN_SEARCH_MODE_DEFAULT;

    /* ---------------------------------------------------------
     * 8. Invoke the opclass’s extractQuery function
     * --------------------------------------------------------- */
    Datum *keys = (Datum *) DatumGetPointer(
            FunctionCall7Coll(&flinfo,
                collation, // DEFAULT_COLLATION_OID or from query
                queryDatum,
                PointerGetDatum(&nkeys),
                UInt16GetDatum(op_strategy_number),
                PointerGetDatum(&partial_matches),
                PointerGetDatum(&extra_data),
                PointerGetDatum(&nullFlags),
                PointerGetDatum(&searchMode)));

    /* ---------------------------------------------------------
     * 9. Pack keys into a C++ vector for return
     * --------------------------------------------------------- */
    std::vector<std::string> result;
    result.reserve(nkeys);
    for (int i = 0; i < nkeys; ++i)
        result.push_back(unpack_trigram_int_to_string(keys[i]));

    return result;
}

std::string unpack_trigram_int_to_string(uint32_t v)
{
    unsigned char b1 = (v >> 16) & 0xFF;
    unsigned char b2 = (v >> 8) & 0xFF;
    unsigned char b3 = v & 0xFF;
    return std::string(reinterpret_cast<char const*>(&b1), 1)
        + std::string(reinterpret_cast<char const*>(&b2), 1)
        + std::string(reinterpret_cast<char const*>(&b3), 1);
}

