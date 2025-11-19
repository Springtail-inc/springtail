#include <cstdint>

#include <pg_ext/common.hh>
#include <pg_ext/export.hh>
#include <pg_ext/numeric.hh>
#include <pg_ext/string.hh>
#include <pg_ext/date.hh>

#include <common/logging.hh>

constexpr uint32_t JB_OFFSET_STRIDE		= 32;

/* flags for the header-field in JsonbContainer */
constexpr uint32_t JB_CMASK				= 0x0FFFFFFF;	/* mask for count field */
constexpr uint32_t JB_FSCALAR			= 0x10000000;	/* flag bits */
constexpr uint32_t JB_FOBJECT			= 0x20000000;
constexpr uint32_t JB_FARRAY			= 0x40000000;

constexpr uint32_t JENTRY_OFFLENMASK		= 0x0FFFFFFF;
constexpr uint32_t JENTRY_TYPEMASK			= 0x70000000;
constexpr uint32_t JENTRY_HAS_OFF			= 0x80000000;

/* values stored in the type bits */
constexpr uint32_t JENTRY_ISSTRING			= 0x00000000;
constexpr uint32_t JENTRY_ISNUMERIC			= 0x10000000;
constexpr uint32_t JENTRY_ISBOOL_FALSE		= 0x20000000;
constexpr uint32_t JENTRY_ISBOOL_TRUE		= 0x30000000;
constexpr uint32_t JENTRY_ISNULL			= 0x40000000;
constexpr uint32_t JENTRY_ISCONTAINER		= 0x50000000;	/* array or object */

/* Access macros.  Note possible multiple evaluations */
#define JBE_OFFLENFLD(je_)		((je_) & JENTRY_OFFLENMASK)
#define JBE_HAS_OFF(je_)		(((je_) & JENTRY_HAS_OFF) != 0)
#define JBE_ISSTRING(je_)		(((je_) & JENTRY_TYPEMASK) == JENTRY_ISSTRING)
#define JBE_ISNUMERIC(je_)		(((je_) & JENTRY_TYPEMASK) == JENTRY_ISNUMERIC)
#define JBE_ISCONTAINER(je_)	(((je_) & JENTRY_TYPEMASK) == JENTRY_ISCONTAINER)
#define JBE_ISNULL(je_)			(((je_) & JENTRY_TYPEMASK) == JENTRY_ISNULL)
#define JBE_ISBOOL_TRUE(je_)	(((je_) & JENTRY_TYPEMASK) == JENTRY_ISBOOL_TRUE)
#define JBE_ISBOOL_FALSE(je_)	(((je_) & JENTRY_TYPEMASK) == JENTRY_ISBOOL_FALSE)
#define JBE_ISBOOL(je_)			(JBE_ISBOOL_TRUE(je_) || JBE_ISBOOL_FALSE(je_))

/* Macro for advancing an offset variable to the next JEntry */
#define JBE_ADVANCE_OFFSET(offset, je) \
	do { \
		JEntry	je_ = (je); \
		if (JBE_HAS_OFF(je_)) \
			(offset) = JBE_OFFLENFLD(je_); \
		else \
			(offset) += JBE_OFFLENFLD(je_); \
	} while(0)

#define JSONB_MAX_ELEMS (ExtMin(MaxAllocSize / sizeof(JsonbValue), JB_CMASK))
#define JSONB_MAX_PAIRS (ExtMin(MaxAllocSize / sizeof(JsonbPair), JB_CMASK))

#define JsonContainerSize(jc)		((jc)->header & JB_CMASK)
#define JsonContainerIsScalar(jc)	(((jc)->header & JB_FSCALAR) != 0)

#define IsAJsonbScalar(jsonbval) \
    ((jsonbval != nullptr) && \
    ((((jsonbval)->type >= jbvType::jbvNull) && ((jsonbval)->type <= jbvType::jbvBool)) || \
     ((jsonbval)->type == jbvType::jbvDatetime)))

/* Forward declarations */
struct JsonbPair;
struct JsonbValue;
struct JsonbContainer;
using JEntry = uint32_t;

enum class JsonTokenType
{
	JSON_TOKEN_INVALID,
	JSON_TOKEN_STRING,
	JSON_TOKEN_NUMBER,
	JSON_TOKEN_OBJECT_START,
	JSON_TOKEN_OBJECT_END,
	JSON_TOKEN_ARRAY_START,
	JSON_TOKEN_ARRAY_END,
	JSON_TOKEN_COMMA,
	JSON_TOKEN_COLON,
	JSON_TOKEN_TRUE,
	JSON_TOKEN_FALSE,
	JSON_TOKEN_NULL,
	JSON_TOKEN_END
};

enum class JsonParseErrorType
{
	JSON_SUCCESS,
	JSON_ESCAPING_INVALID,
	JSON_ESCAPING_REQUIRED,
	JSON_EXPECTED_ARRAY_FIRST,
	JSON_EXPECTED_ARRAY_NEXT,
	JSON_EXPECTED_COLON,
	JSON_EXPECTED_END,
	JSON_EXPECTED_JSON,
	JSON_EXPECTED_MORE,
	JSON_EXPECTED_OBJECT_FIRST,
	JSON_EXPECTED_OBJECT_NEXT,
	JSON_EXPECTED_STRING,
	JSON_INVALID_TOKEN,
	JSON_UNICODE_CODE_POINT_ZERO,
	JSON_UNICODE_ESCAPE_FORMAT,
	JSON_UNICODE_HIGH_ESCAPE,
	JSON_UNICODE_UNTRANSLATABLE,
	JSON_UNICODE_HIGH_SURROGATE,
	JSON_UNICODE_LOW_SURROGATE,
	JSON_SEM_ACTION_FAILED		/* error should already be reported */
};

enum class JsonbIteratorToken
{
	WJB_DONE,
	WJB_KEY,
	WJB_VALUE,
	WJB_ELEM,
	WJB_BEGIN_ARRAY,
	WJB_END_ARRAY,
	WJB_BEGIN_OBJECT,
	WJB_END_OBJECT
};

enum class JsonbIterState
{
	JBI_ARRAY_START,
	JBI_ARRAY_ELEM,
	JBI_OBJECT_START,
	JBI_OBJECT_KEY,
	JBI_OBJECT_VALUE
};

enum class jbvType {
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


#define JSON_ALPHANUMERIC_CHAR(c)  \
	(((c) >= 'a' && (c) <= 'z') || \
	 ((c) >= 'A' && (c) <= 'Z') || \
	 ((c) >= '0' && (c) <= '9') || \
	 (c) == '_' || \
	 IS_HIGHBIT_SET(c))

/* Forward declare the structs that will be used in the union */
struct JsonbValue;

struct JsonbPair {
    JsonbValue *key;   /* Must be a jbvString */
    JsonbValue *value; /* May be of any type */
    uint32_t order;    /* Pair's index in original sequence */
};

struct JsonbContainer {
    uint32_t header; /* number of elements or key/value pairs, and
                      * flags */
    JEntry children[FLEXIBLE_ARRAY_MEMBER];

    /* the data for each child node follows. */
};

struct Jsonb
{
	int32_t		vl_len_;		/* varlena header (do not touch directly!) */
	JsonbContainer root;
};
struct JsonbValue {
    jbvType type; /* Influences sort order */

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
struct JsonbParseState
{
	JsonbValue	contVal;
	Size		size;
	struct JsonbParseState *next;
	bool		unique_keys;	/* Check object key uniqueness */
	bool		skip_nulls;		/* Skip null object fields */
};

struct JsonbIterator
{
	/* Container being iterated */
	JsonbContainer *container;
	uint32_t nElems;			/* Number of elements in children array (will
								 * be nPairs for objects) */
	bool isScalar;		/* Pseudo-array scalar value? */
	JEntry *children;		/* JEntrys for child nodes */
	/* Data proper.  This points to the beginning of the variable-length data */
	char *dataProper;

	/* Current item in buffer (up to nElems) */
	int curIndex;

	/* Data offset corresponding to current item */
	uint32_t curDataOffset;

	/*
	 * If the container is an object, we want to return keys and values
	 * alternately; so curDataOffset points to the current key, and
	 * curValueOffset points to the current value.
	 */
     uint32_t curValueOffset;

	/* Private state */
	JsonbIterState state;

	struct JsonbIterator *parent;
};

struct JsonLexContext
{
	char	   *input;
	int			input_length;
	int			input_encoding;
	char	   *token_start;
	char	   *token_terminator;
	char	   *prev_token_terminator;
	JsonTokenType token_type;
	int			lex_level;
	int			line_number;	/* line number, starting from 1 */
	char	   *line_start;		/* where that line starts within input */
	StringInfo	strval;
};

extern "C" PGEXT_API JsonbValue * pushJsonbValue(JsonbParseState **pstate, JsonbIteratorToken seq, JsonbValue *jbval);
extern "C" PGEXT_API bool IsValidJsonNumber(const char *str, int len);
extern "C" PGEXT_API Jsonb *JsonbValueToJsonb(JsonbValue *val);
extern "C" PGEXT_API void escape_json(StringInfo buf, const char *str);
extern "C" PGEXT_API void convertJsonbValue(StringInfo buffer, JEntry *header, JsonbValue *val, int level);
extern "C" PGEXT_API Jsonb *convertToJsonb(JsonbValue *val);
extern "C" PGEXT_API void convertJsonbScalar(StringInfo buffer, JEntry *header, JsonbValue *scalarVal);
extern "C" PGEXT_API short padBufferToInt(StringInfo buffer);
extern "C" PGEXT_API void convertJsonbArray(StringInfo buffer, JEntry *header, JsonbValue *val, int level);
extern "C" PGEXT_API void convertJsonbObject(StringInfo buffer, JEntry *header, JsonbValue *val, int level);
