
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <fstream>
#include <unordered_map>
#include <nlohmann/json.hpp>
extern "C" {
    #include "pg_query.h"
}

std::vector<std::string> WHITELIST_EXTNS = {"pg_trgm"};

using DataMap = std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>>;

void
dump_parse_tree(const std::string &query)
{
    PgQueryParseResult result = pg_query_parse(query.c_str());

    if (result.error) {
        printf("error: %s at %d\n", result.error->message, result.error->cursorpos);
    } else {
        printf("%s\n", result.parse_tree);
    }

    pg_query_free_parse_result(result);
}

std::string clean_sql_content(const std::string& sql) {
    std::istringstream stream(sql);
    std::string line;
    std::string clean_sql;
    bool in_comment = false;

    while (std::getline(stream, line)) {
        // Skip empty lines
        if (line.empty()) continue;

        // Handle multi-line comments
        size_t comment_pos = line.find("/*");
        if (comment_pos != std::string::npos) {
            size_t comment_end = line.find("*/", comment_pos);
            if (comment_end != std::string::npos) {
                line = line.substr(0, comment_pos) + line.substr(comment_end + 2);
            } else {
                in_comment = true;
                line = line.substr(0, comment_pos);
            }
        } else if (in_comment) {
            size_t comment_end = line.find("*/");
            if (comment_end != std::string::npos) {
                in_comment = false;
                line = line.substr(comment_end + 2);
            } else {
                continue;
            }
        }

        // Skip psql commands (lines starting with \)
        if (!line.empty() && line[0] == '\\') {
            continue;
        }

        // Remove single-line comments
        size_t dash_comment = line.find("--");
        if (dash_comment != std::string::npos) {
            line = line.substr(0, dash_comment);
        }

        // Only add non-empty lines
        if (!line.empty()) {
            clean_sql += line + "\n";
        }
    }

    return clean_sql;
}

void parse_function(const nlohmann::json& st, DataMap& functions){
    if (st.contains("CreateFunctionStmt")) {
        const auto& func = st["CreateFunctionStmt"];
        std::string func_name;
        std::vector<std::pair<std::string, std::string>> args;

        if (func.contains("funcname")) {
            for (const auto& n : func["funcname"]) {
                if (n.contains("String")) {
                    func_name += n["String"]["sval"].get<std::string>() + ".";
                }
            }
            if (!func_name.empty()) func_name.pop_back();
        }

        if (func.contains("parameters")) {
            for (const auto& param : func["parameters"]) {
                if (param.contains("FunctionParameter")) {
                    const auto& fp = param["FunctionParameter"];
                    std::string type;
                    if (fp.contains("argType")) {
                        const auto& tn = fp["argType"];
                        if (tn.contains("names")) {
                            for (const auto& n : tn["names"]) {
                                if (n.contains("String")) {
                                    type += n["String"]["sval"].get<std::string>() + ".";
                                }
                            }
                            if (!type.empty()) type.pop_back();
                        }
                    }
                    args.push_back(std::make_pair(type, ""));
                }
            }
        }

        functions[func_name] = args;
    }
}

void parse_operator(const nlohmann::json& st, DataMap& operators){
    if (st.contains("DefineStmt")) {
        const auto& def = st["DefineStmt"];
        const auto& kind = def["kind"];
        if (kind == "OBJECT_OPERATOR") {
            std::vector<std::pair<std::string, std::string>> args;
            std::string op_name;
            if (def.contains("defnames")){
                const auto& opname = def["defnames"][0]["String"]["sval"].get<std::string>();
                op_name = opname;
            }
            if (def.contains("definition")) {
                const auto& defn = def["definition"];
                for (const auto& d : defn) {
                    if (d.contains("DefElem")) {
                        const auto& de = d["DefElem"];
                        const auto& defname = de["defname"];
                        if (de.contains("arg")) {
                            const auto& arg = de["arg"];
                            if (arg.contains("TypeName")) {
                                const auto& tn = arg["TypeName"];
                                if (tn.contains("names")) {
                                    for (const auto& n : tn["names"]) {
                                        if (n.contains("String")) {
                                            args.push_back(std::make_pair(defname, n["String"]["sval"]));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            operators[op_name] = args;
        }
    }
}

// XXX: Handle alter and versioning
void parse_sql(const std::string& sql, DataMap& functions, DataMap& operators) {
    std::string clean_sql = clean_sql_content(sql);

    if (clean_sql.empty()) {
        return;
    }

    PgQueryParseResult result = pg_query_parse(clean_sql.c_str());
    if (result.error) {
        std::cerr << "Parse error: " << result.error->message << std::endl;
        pg_query_free_parse_result(result);
        return;
    }

    nlohmann::json parse_tree = nlohmann::json::parse(result.parse_tree);

    for (const auto& stmt : parse_tree["stmts"]) {
        auto st = stmt["stmt"];
        parse_function(st, functions);
        parse_operator(st, operators);
    }

    pg_query_free_parse_result(result);
}

std::string read_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("Unable to read file: " + path);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

void print_functions(const DataMap& functions){
    std::cout << "Functions:\n";
    for (const auto& func : functions) {
        std::cout << "FUNC: " << func.first << "(";
        for (const auto& [arg, type] : func.second) {
            std::cout << arg << ", ";
        }
        std::cout << ")\n";
    }
}

void print_operators(const DataMap& operators){
    std::cout << "Operators:\n";
    for (const auto& func : operators) {
        std::cout << "OP: " << func.first << std::endl;
        for (const auto& arg : func.second) {
            std::cout << "ARG: " << arg.first << ", TYPE: " << arg.second << std::endl;
        }
    }
}

void process_extension_dir(const std::filesystem::path& ext_dir) {
    std::map<std::string, std::vector<std::string>> versioned_files;

    for (const auto& entry : std::filesystem::directory_iterator(ext_dir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".sql") {
            std::string file_name = entry.path().filename().string();
            size_t version_pos = file_name.find("-");
            if (version_pos != std::string::npos) {
                std::string extn = file_name.substr(0, version_pos);
                if (std::find(WHITELIST_EXTNS.begin(), WHITELIST_EXTNS.end(), extn) != WHITELIST_EXTNS.end()) {
                    versioned_files[extn].push_back(entry.path().string());
                }
            }
        }
    }

    DataMap functions;
    DataMap operators;

    for (const auto& [extn, files] : versioned_files) {
        std::cout << "Extension: " << extn << std::endl;
        for (const auto& file : files) {
            std::cout << "  " << file << std::endl;
            std::string sql = read_file(file);
            parse_sql(sql, functions, operators);
        }
    }

    print_functions(functions);
    print_operators(operators);
}

int
main()
{
    std::string base_dir = "/usr/share/postgresql/16/extension";

    process_extension_dir(base_dir);

    return 0;
}
