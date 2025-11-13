#include <pg_ext/trgm_helpers.hh>

namespace springtail {

    std::string unpack_trigram_int_to_string(uint32_t v)
    {
        unsigned char b1 = (v >> 16) & 0xFF;
        unsigned char b2 = (v >> 8) & 0xFF;
        unsigned char b3 = v & 0xFF;
        return std::string(reinterpret_cast<char const*>(&b1), 1)
            + std::string(reinterpret_cast<char const*>(&b2), 1)
            + std::string(reinterpret_cast<char const*>(&b3), 1);
    }

    void* cstring_to_text_4b(const char *s)
    {
        if (!s) {
            LOG_ERROR("Invalid arguments to cstring_to_text_4b");
            return nullptr;
        }
        size_t data_len = std::strlen(s);
        size_t tot = VARHDRSZ + data_len;
        char *p = (char*)std::malloc(tot);
        if (!p) {
            std::perror("malloc");
            std::exit(1);
        }
        SET_VARSIZE_4B(p, data_len + VARHDRSZ);
        std::memcpy(p + VARHDRSZ, s, data_len);
        return (void*)p;
    }

    void* cstring_to_text_1b(const char *s)
    {
        if (!s) {
            LOG_ERROR("Invalid arguments to cstring_to_text_1b");
            return nullptr;
        }
        size_t data_len = std::strlen(s);
        size_t tot = VARHDRSZ + data_len;
        char *p = (char*)std::malloc(tot);
        if (!p) {
            std::perror("malloc");
            std::exit(1);
        }
        SET_VARSIZE_1B(p, data_len + VARHDRSZ_SHORT);
        std::memcpy(p + VARHDRSZ_SHORT, s, data_len);
        return (void*)p;
    }

    void* cstring_to_text_auto(const char *s)
    {
        if (!s) {
            LOG_ERROR("Invalid arguments to cstring_to_text_auto");
            return nullptr;
        }
        size_t n = std::strlen(s);
        return (n <= 127) ? cstring_to_text_1b(s) : cstring_to_text_4b(s);
    }

    std::vector<std::string> extract_trgm_from_value(const std::string& value, const std::string& opclass, int method_strategy_number)
    {
        auto* input_string = cstring_to_text_auto(value.c_str());

        auto&& extract_func = PgExtnRegistry::get_instance()->get_opclass_method_func_ptr_by_method_name(opclass, method_strategy_number);
        LOCAL_FCINFO(fcinfo, 2);

        InitFunctionCallInfoData(*fcinfo, nullptr, 2, 0, nullptr, nullptr);

        fcinfo->args[0].value = PointerGetDatum(input_string);
        fcinfo->args[0].isnull = false;

        int32_t nentries = 0;
        fcinfo->args[1].value = PointerGetDatum(&nentries);
        fcinfo->args[1].isnull = false;

        Datum result = extract_func(fcinfo);
        Datum *entries = reinterpret_cast<Datum *>(DatumGetPointer(result));
        std::vector<std::string> out;

        for (int i = 0; i < nentries; ++i)
        {
            // packed int (trgm2int result)
            int32_t packed = DatumGetInt32(entries[i]);
            uint32_t u = static_cast<uint32_t>(packed);
            // 3-byte string
            out.push_back(unpack_trigram_int_to_string(u));
        }

        return out;
    }
}
