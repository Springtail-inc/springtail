#include <pg_ext/float.hh>

#include <cstdlib>

char * float8out_internal(double num) {
    return (char *) std::malloc(256);
}

double float8in_internal(const char *numstr) {
    // XXX Stubbed for now
    return 0.0;
}
