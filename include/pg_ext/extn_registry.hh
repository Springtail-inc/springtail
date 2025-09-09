#include <fmt/format.h>
#include <libpq-fe.h>

#include <cstdint>
#include <string>
#include <unordered_map>

#include <common/logging.hh>
#include <pg_ext/fmgr.hh>
#include <pg_repl/libpq_connection.hh>

namespace springtail {
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

    void init_libraries(uint64_t db_id, const std::string& extension, const std::string& extension_lib_path);
    PgType get_type_by_oid(uint32_t oid) const;
    void add_type(const std::string& extension, uint32_t oid, const std::string& typinput, const std::string& typoutput, const std::string& typreceive, const std::string& typsend);
    void add_operator(const std::string& extension, uint32_t oid, const std::string& oper_name, const std::string& oper_proc);

    PGFunction get_operator_func_by_oid(uint32_t oid) const;
    PGFunction get_operator_func_by_oper_name(const std::string& oper_name) const;
    PGFunction get_operator_func_by_proc_name(const std::string& proc_name) const;
    PGFunction get_type_func_by_type_name(const std::string& type_name) const;

    std::string datum_to_string(Datum value, Oid pg_oid);
    Datum binary_to_datum(const std::span<const char> &value,
                          Oid pg_oid,
                          int32_t atttypmod);
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
