#include <pg_ext/common.hh>
#include <pg_ext/type.hh>

char
get_typtype(Oid typid)
{
    // XXX Stubbed for now
    return '\0';
}

Oid
getBaseTypeAndTypmod(Oid typid, int32_t *typmod)
{
    // XXX Stubbed for now
    return typid;
}

Oid
getBaseType(Oid typid)
{
    int32_t typmod = -1;

    return getBaseTypeAndTypmod(typid, &typmod);
}

bool
type_is_rowtype(Oid typid)
{
    // RECORDOID XXX Stubbed for now
    if (typid == -1) {
        return true; /* easy case */
    }
    switch (get_typtype(typid)) {
        case TYPTYPE_COMPOSITE:
            return true;
        case TYPTYPE_DOMAIN:
            if (get_typtype(getBaseType(typid)) == TYPTYPE_COMPOSITE) {
                return true;
            }
            break;
        default:
            break;
    }
    return false;
}
