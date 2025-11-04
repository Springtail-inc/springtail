#include <pg_ext/domains.hh>


void domain_check(Oid domainoid, Datum value, bool isnull)
{
    // XXX Stubbed for now
}

void domain_check_input(Datum value,
                        bool isnull,
                        DomainIOData *my_extra,
                        Node *escontext)
{
    // XXX Stubbed for now
}
