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

    std::vector<std::string> extract_trgm_from_value(const std::string& value, const std::string& opclass, int method_strategy_number)
    {
        auto* input_string = cstring_to_text_auto(value.c_str());

        auto&& extract_func = (PGFunction) PgExtnRegistry::get_instance()->get_opclass_method_func_ptr_by_method_name(opclass, method_strategy_number);
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
