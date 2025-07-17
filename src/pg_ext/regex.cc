#include <pg_ext/regex.hh>
#include <regex.h>

int pg_regcomp(regex_t *regex, const char *pattern, int cflags) {
    return regcomp(regex, pattern, cflags);
}

int pg_regerror(int errcode, const regex_t *regex, char *errbuf, size_t errbuf_size) {
    return regerror(errcode, regex, errbuf, errbuf_size);
}

int pg_reg_getinitialstate(const regex_t *regex) {
    return regex->re_nsub;
}

int pg_reg_getfinalstate(const regex_t *regex) {
    return regex->re_nsub;
}

int pg_reg_getoutarcs(const regex_t *regex, int state, int *arcs) {
    return 0;
}

int pg_reg_getnumoutarcs(const regex_t *regex, int state) {
    return 0;
}

int pg_reg_getcharacters(const regex_t *regex, int arc, int *chars) {
    return 0;
}

int pg_reg_getnumcharacters(const regex_t *regex, int arc) {
    return 0;
}

int pg_reg_getnumcolors(const regex_t *regex) {
    return 0;
}

int pg_reg_colorisbegin(const regex_t *regex, int color) {
    return 0;
}

int pg_reg_colorisend(const regex_t *regex, int color) {
    return 0;
}
