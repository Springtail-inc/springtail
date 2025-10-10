#include <fmt/format.h>
#include <libpq-fe.h>

#include <cstdint>
#include <string>
#include <unordered_map>

#include <common/logging.hh>
#include <pg_ext/fmgr.hh>
#include <pg_repl/libpq_connection.hh>

namespace springtail {
// Minimal replication of pg_type ( Form_pg_type )
struct PgType {
    uint32_t oid;
    std::string typinput;
    std::string typoutput;
    std::string typreceive;
    std::string typsend;
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
     * Get the operator function by oid
     * @param oid The operator oid
     * @return The operator function
     */
    PGFunction get_operator_func_by_oid(uint32_t oid) const;
    /**
     * Get the operator function by operator name
     * @param oper_name The operator name
     * @return The operator function
     */
    PGFunction get_operator_func_by_oper_name(const std::string& oper_name) const;
    /**
     * Get the operator function by proc name
     * @param proc_name The proc name
     * @return The operator function
     */
    PGFunction get_operator_func_by_proc_name(const std::string& proc_name) const;
    /**
     * Get the type function by type name
     * @param type_name The type name
     * @return The type function
     */
    PGFunction get_type_func_by_type_name(const std::string& type_name) const;

    /**
     * Convert a datum to a string - Using the typeouput function of the extension type
     * @param value The datum
     * @param pg_oid The oid of the type
     * @return The string
     */
    std::string datum_to_string(Datum value, Oid pg_oid);
    /**
     * Convert a binary value to a datum - Using the typreceive function of the extension type
     * @param value The binary value
     * @param pg_oid The oid of the type
     * @param atttypmod The type modifier
     * @return The datum
     */
    Datum binary_to_datum(const std::span<const char> &value,
                          Oid pg_oid,
                          int32_t atttypmod);
    /**
     * Compare two datums - Using the comparator function of the extension type
     * Looks up the comparator function using the type oid and operation name string
     * @param type_oid The oid of the type
     * @param op_str The operator string
     * @param lhval The left hand value
     * @param rhval The right hand value
     * @return The result of the comparison
     */
    static bool comparator_func(uint64_t type_oid,
                                std::string_view op_str,
                                const std::span<const char> &lhval,
                                const std::span<const char> &rhval);
private:
    PGFunction _load_extn_function(void* library, const std::string_view oper_name);
    void* _load_library(const std::string_view lib_path);

    PgExtnRegistry() {}
    uint64_t _db_id;
    uint64_t _xid;

    std::unordered_map<uint32_t, std::string> _oper_oid_to_name;
    std::unordered_map<uint32_t, std::string> _proc_oid_to_name;
    std::unordered_map<uint32_t, PgType> _type_oid_to_type;

    std::unordered_map<std::string, PGFunction> _oper_name_to_func;
    std::unordered_map<std::string, PGFunction> _proc_name_to_func;
    std::unordered_map<std::string, PGFunction> _type_func_name_to_func;

    std::unordered_map<std::string, void*> _library_map;
};
}  // namespace springtail
