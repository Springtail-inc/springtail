#pragma once

#include "export.hh"
#include <vector>
#include <string>

typedef int regcolor_t;
typedef int regstate_t;

struct regex_arc_t {
    regstate_t from;
    regstate_t to;
    regcolor_t color;
};

struct regex_t {
    std::string pattern;
    std::vector<regex_arc_t> arcs;
    std::vector<regcolor_t> colors;
    regstate_t initial;
    regstate_t final;
    std::string error;
};

//// EXPORTED INTERFACES
extern "C" PGEXT_API int pg_regcomp(regex_t *regex, const char *pattern, int cflags);
extern "C" PGEXT_API int pg_regerror(int errcode, const regex_t *regex, char *errbuf, size_t errbuf_size);
extern "C" PGEXT_API regstate_t pg_reg_getinitialstate(const regex_t *regex);
extern "C" PGEXT_API regstate_t pg_reg_getfinalstate(const regex_t *regex);
extern "C" PGEXT_API const regex_arc_t *pg_reg_getoutarcs(const regex_t *regex, int state, int *arcs);
extern "C" PGEXT_API int pg_reg_getnumoutarcs(const regex_t *regex, int state);
extern "C" PGEXT_API const regcolor_t pg_reg_getcharacters(const regex_t *regex, int arc, int *chars);
extern "C" PGEXT_API int pg_reg_getnumcharacters(const regex_t *regex, int arc);
extern "C" PGEXT_API int pg_reg_getnumcolors(const regex_t *regex);
extern "C" PGEXT_API bool pg_reg_colorisbegin(const regex_t *regex, int color);
extern "C" PGEXT_API bool pg_reg_colorisend(const regex_t *regex, int color);
