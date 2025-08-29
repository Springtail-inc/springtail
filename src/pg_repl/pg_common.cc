#include <common/constants.hh>
#include <pg_repl/pg_common.hh>

extern "C" {
    #include <postgres.h>
    #include <catalog/pg_type.h>
}

namespace springtail
{
    SchemaType
    convert_pg_type(int32_t pg_type, char pg_type_category)
    {
        switch (pg_type) {
            case INT4OID:
            case DATEOID:
                return SchemaType::INT32;

            case TEXTOID:
            case VARCHAROID:
            case BPCHAROID:
                return SchemaType::TEXT;

            case INT8OID:
            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
            case TIMEOID:
            case MONEYOID:
                return SchemaType::INT64;

            case BOOLOID:
                return SchemaType::BOOLEAN;

            case INT2OID:
                return SchemaType::INT16;

            case FLOAT4OID:
                return SchemaType::FLOAT32;

            case FLOAT8OID:
                return SchemaType::FLOAT64;

            case CHAROID:
                return SchemaType::INT8;

            case NUMERICOID:
                return SchemaType::NUMERIC;

            default:
                if (pg_type_category == constant::USER_TYPE_ENUM) {
                    // enum types; treat as REAL/FLOAT4, to store the enum index
                    return SchemaType::FLOAT32;
                }
                if (pg_type_category == constant::USER_TYPE_EXTENSION) {
                    // extension types; treat as custom BINARY type
                    return SchemaType::BINARY;
                }
                // put all other types into BINARY data for now
                return SchemaType::BINARY;
        }
    }

}
