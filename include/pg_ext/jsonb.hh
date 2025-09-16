#include <cstdint>

#include <pg_ext/common.hh>
#include <pg_ext/export.hh>
#include <pg_ext/numeric.hh>
#include <pg_ext/string.hh>

/* Forward declarations */
typedef struct JsonbPair JsonbPair;
typedef struct JsonbValue JsonbValue;
typedef struct JsonbContainer JsonbContainer;
typedef uint32_t JEntry;

enum jbvType {
    /* Scalar types */
    jbvNull = 0x0,
    jbvString,
    jbvNumeric,
    jbvBool,
    /* Composite types */
    jbvArray = 0x10,
    jbvObject,
    /* Binary (i.e. struct Jsonb) jbvArray/jbvObject */
    jbvBinary,

    /*
     * Virtual types.
     *
     * These types are used only for in-memory JSON processing and serialized
     * into JSON strings when outputted to json/jsonb.
     */
    jbvDatetime = 0x20,
};

/* Forward declare the structs that will be used in the union */
struct JsonbValue;

struct JsonbPair {
    JsonbValue *key;   /* Must be a jbvString */
    JsonbValue *value; /* May be of any type */
    uint32_t order;    /* Pair's index in original sequence */
};

typedef struct JsonbContainer {
    uint32_t header; /* number of elements or key/value pairs, and
                      * flags */
    JEntry children[FLEXIBLE_ARRAY_MEMBER];

    /* the data for each child node follows. */
} JsonbContainer;

struct JsonbValue {
    enum jbvType type; /* Influences sort order */

    union {
        Numeric numeric;
        bool boolean;
        struct {
            int len;
            char *val; /* Not necessarily null-terminated */
        } string;      /* String primitive type */

        struct {
            int nElems;
            JsonbValue *elems;
            bool rawScalar; /* Top-level "raw scalar" array? */
        } array;            /* Array container type */

        struct {
            int nPairs; /* 1 pair, 2 elements */
            JsonbPair *pairs;
        } object; /* Associative container type */

        struct {
            int len;
            JsonbContainer *data;
        } binary; /* Array or object, in on-disk format */

        struct {
            Datum value;
            Oid typid;
            int32_t typmod;
            int tz; /* Numeric time zone, in seconds, for
                     * TimestampTz data type */
        } datetime;
    } val;
};

extern "C" PGEXT_API JsonbValue *pushJsonbValue(JsonbValue *val);
extern "C" PGEXT_API bool IsValidJsonNumber(const char *str, int len);
extern "C" PGEXT_API JsonbValue *JsonbValueToJsonb(JsonbValue *val);
extern "C" PGEXT_API void escape_json(StringInfo buf, const char *str);
