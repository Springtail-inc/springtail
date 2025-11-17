#pragma once

#include <fmt/format.h>
#include <libpq-fe.h>

#include <cstdint>
#include <string>
#include <unordered_map>

#include <common/logging.hh>
#include <common/constants.hh>
#include <common/libpq_connection.hh>
#include <pg_ext/fmgr.hh>

namespace springtail {
// Minimal replication of pg_type ( Form_pg_type )
struct PgType {
    uint32_t oid;
    std::string typinput;
    std::string typoutput;
    std::string typreceive;
    std::string typsend;
};

/**
 * Struct containing the opclass method details
 * input_type_oid: Oid of the input type needed to convert data to datum
 * input_type: Name of the input type
 * key_type_oid: Oid of the key type needed to convert data to datum
 * key_type: Name of the key type
 * support_number: Support number of the method ( defined in constants.hh )
 * function_name: Name of the function ( references pg_proc -> proname )
 * function_ptr: Function pointer Pointer to the function
 */
struct PgOpsClassMethod {
    uint32_t input_type_oid;
    std::string input_type;
    uint32_t key_type_oid;
    std::string key_type;
    int support_number;
    std::string function_name;
    void* function_ptr = nullptr;
};

/**
 * Struct containing the opclass details
 * oid: Oid of the opclass ( references pg_opclass -> oid )
 * name: Name of the opclass ( references pg_opclass -> opcname )
 * schema: Schema of the opclass ( references pg_namespace -> nspname )
 * access_method: Access method of the opclass ( references pg_am -> amname ) ( GIN/GIST for ex )
 * family: Family of the opclass ( references pg_opfamily -> opfname)
 */
struct PgOpsClass {
    uint32_t oid;
    std::string name;
    std::string schema;
    std::string access_method;
    std::string family;
};

class PgExtnRegistry : public Singleton<PgExtnRegistry> {
    friend class Singleton<PgExtnRegistry>;

public:
    static constexpr char OPER_QUERY[] =
        "SELECT"
        "   opr.oid          AS oper_oid, "
        "   opr.oprname      AS oper_name, "
        "   proc.oid         AS proc_oid, "
        "   proc.proname     AS proc_name, "
        "   proc.proargtypes AS arg_types, "
        "   proc.prorettype  AS ret_type "
        "FROM pg_operator opr "
        "JOIN pg_proc proc "
        "  ON proc.oid = opr.oprcode "
        "WHERE proc.oid IN ( "
        "   SELECT objid "
        "   FROM pg_depend d "
        "   JOIN pg_extension e ON e.oid = d.refobjid "
        "   WHERE e.extname = '{}' "
        "   AND d.deptype = 'e' "
        "   AND d.classid = 'pg_proc'::regclass "
        ") "
        "ORDER BY opr.oprname;";

    static constexpr char TYPE_QUERY[] =
        "SELECT"
        "   t.oid          AS type_oid, "
        "   split_part(t.typinput::regproc::text, '.'::text, 2) AS type_input, "
        "   split_part(t.typoutput::regproc::text, '.'::text, 2) AS type_output, "
        "   split_part(t.typreceive::regproc::text, '.'::text, 2) AS type_receive, "
        "   split_part(t.typsend::regproc::text, '.'::text, 2) AS type_send "
        "FROM pg_type t "
        "WHERE t.oid IN ( "
        "   SELECT objid "
        "   FROM pg_depend d "
        "   JOIN pg_extension e ON e.oid = d.refobjid "
        "   WHERE e.extname = '{}' "
        "   AND d.deptype = 'e' "
        "   AND d.classid = 'pg_type'::regclass "
        ") "
        "ORDER BY t.typname;";

    static constexpr char OPCLASS_QUERY[] =
        "SELECT"
        "    ext.extname AS extension_name, "
        "    am.amname AS access_method, "
        "    opc.oid AS opclass_oid, "
        "    opc.opcname AS opclass_name, "
        "    opf.opfname AS opfamily_name, "
        "    ns.nspname AS opclass_schema, "
        "    opc.opcintype AS input_type_oid, "
        "    opc.opcintype::regtype AS input_type, "
        "    opc.opckeytype AS key_type_oid, "
        "    opc.opckeytype::regtype AS key_type, "
        "    ap.amprocnum AS support_number, "
        "    p.proname AS support_function_name "
        "FROM "
        "    pg_opclass opc "
        "    JOIN pg_namespace ns ON ns.oid = opc.opcnamespace "
        "    JOIN pg_am am ON am.oid = opc.opcmethod "
        "    LEFT JOIN pg_opfamily opf ON opf.oid = opc.opcfamily "
        "    LEFT JOIN pg_amproc ap ON ap.amprocfamily = opf.oid "
        "    LEFT JOIN pg_proc p ON p.oid = ap.amproc "
        "    LEFT JOIN pg_depend d ON d.objid = opc.oid AND d.classid = 'pg_opclass'::regclass "
        "    LEFT JOIN pg_extension ext ON ext.oid = d.refobjid "
        "WHERE "
        "    am.amname IN ('gin', 'gist') "
        "    AND ext.extname = '{}' "
        "ORDER BY"
        "    opclass_name, support_number; ";

    /**
     * Initialize the libraries for the extension
     * @param db_id The database id
     * @param extension The extension name
     * @param extension_lib_path The path to the extension library
     */
    void init_libraries(uint64_t db_id, const std::string& extension, const std::string& extension_lib_path);
    /**
     * Get the type by oid
     * @param oid The type oid
     * @return The type
     */
    PgType get_type_by_oid(uint32_t oid) const;
    /**
     * Add a type to the registry
     * @param extension The extension name
     * @param oid The type oid
     * @param typinput The type input
     * @param typoutput The type output
     * @param typreceive The type receive
     * @param typsend The type send
     */
    void add_type(const std::string& extension, uint32_t oid, const std::string& typinput, const std::string& typoutput, const std::string& typreceive, const std::string& typsend);
    /**
     * Add an operator to the registry
     * @param extension The extension name
     * @param oid The operator oid
     * @param oper_name The operator name
     * @param oper_proc The operator proc
     */
    void add_operator(const std::string& extension, uint32_t oid, const std::string& oper_name, const std::string& oper_proc);
    /**
     * Add an operator class to the registry
     * @param extension The extension name
     * @param opclass The operator class
     * @param method The operator class method
     */
    void add_opclass(const std::string& extension, PgOpsClass opclass, PgOpsClassMethod method);

    /**
     * Get the operator function by oid
     * @param oid The operator oid
     * @return The operator function
     */
    void* get_operator_func_by_oid(uint32_t oid) const;
    /**
     * Get the operator function by operator name
     * @param oper_name The operator name
     * @return The operator function
     */
    void* get_operator_func_by_oper_name(const char* oper_name) const;
    /**
     * Get the operator function by proc name
     * @param proc_name The proc name
     * @return The operator function
     */
    void* get_operator_func_by_proc_name(const std::string& proc_name) const;
    /**
     * Get the type function by type name
     * @param type_name The type name
     * @return The type function
     */
    void* get_type_func_by_type_name(const std::string& type_name) const;
    /**
     * Get the opclass method by opclass name and support number
     * @param opclass_name The opclass name
     * @param support_number The support number of the method ( defined constants.hh )
     * @return The opclass method
     */
    PgOpsClassMethod get_opclass_method_by_method_name(const std::string& opclass_name,
                                                       int support_number);
    /**
     * Get the opclass method function pointer by opclass name and support number
     * @param opclass_name The opclass name
     * @param support_number The support number of the method ( defined constants.hh )
     * @return The opclass method function pointer
     */
    static void* get_opclass_method_func_ptr_by_method_name(const std::string& opclass_name,
                                                            int support_number);



    /**
     * Convert a datum to a string - Using the typeouput function of the extension type
     * @param value The datum
     * @param pg_oid The oid of the type
     * @return The string
     */
    std::string datum_to_string(Datum value, Oid pg_oid) const;

    /**
     * Convert a binary value to a datum - Using the typreceive function of the extension type
     * @param value The binary value
     * @param pg_oid The oid of the type
     * @param atttypmod The type modifier
     * @return The datum
     */
    Datum binary_to_datum(const std::span<const char> &value,
                          Oid pg_oid,
                          int32_t atttypmod) const;
    /**
     * Compare two datums - Using the comparator function of the extension type
     * Looks up the comparator function using the type oid and operation name string
     * @param context The comparator context
     * @param lhval The left hand value
     * @param rhval The right hand value
     * @return The result of the comparison
     */
    static bool comparator_func(const ExtensionContext* context,
                                const std::span<const char> &lhval,
                                const std::span<const char> &rhval);
private:
    void* _load_extn_function(void* library, const std::string_view oper_name);
    void* _load_library(const std::string_view lib_path);

    PgExtnRegistry() = default;
    uint64_t _db_id;
    uint64_t _xid;

    std::unordered_map<uint32_t, std::string> _oper_oid_to_name;
    std::unordered_map<uint32_t, std::string> _proc_oid_to_name;
    std::unordered_map<uint32_t, PgType> _type_oid_to_type;

    std::unordered_map<std::string, void*> _oper_name_to_func;
    std::unordered_map<std::string, void*> _proc_name_to_func;
    std::unordered_map<std::string, void*> _type_func_name_to_func;

    // Map of opclass name to a map of support_number to function pointer
    std::unordered_map<std::string, std::unordered_map<int, PgOpsClassMethod>> _opclass_function_map;

    std::unordered_map<std::string, void*> _library_map;
};
}  // namespace springtail
