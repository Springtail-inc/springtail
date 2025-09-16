#include <pg_ext/jsonb.hh>
#include <pg_ext/string.hh>

JsonbValue *pushJsonbValue(JsonbValue *val) {
    // XXX Stubbed for now
    return val;
}

bool IsValidJsonNumber(const char *str, int len) {
    // XXX Stubbed for now
    return true;
}

JsonbValue *JsonbValueToJsonb(JsonbValue *val) {
    // XXX Stubbed for now
    return val;
}

void
escape_json(StringInfo buf, const char *str)
{
    const char *p;

    appendStringInfoCharMacro(buf, '"');
    for (p = str; *p; p++) {
        switch (*p) {
            case '\b':
                appendStringInfoString(buf, "\\b");
                break;
            case '\f':
                appendStringInfoString(buf, "\\f");
                break;
            case '\n':
                appendStringInfoString(buf, "\\n");
                break;
            case '\r':
                appendStringInfoString(buf, "\\r");
                break;
            case '\t':
                appendStringInfoString(buf, "\\t");
                break;
            case '"':
                appendStringInfoString(buf, "\\\"");
                break;
            case '\\':
                appendStringInfoString(buf, "\\\\");
                break;
            default:
                if ((unsigned char)*p < ' ') {
                    appendStringInfo(buf, "\\u%04x", (int)*p);
                } else {
                    appendStringInfoCharMacro(buf, *p);
                }
                break;
        }
    }
    appendStringInfoCharMacro(buf, '"');
}
