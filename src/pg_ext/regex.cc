#include <pg_ext/regex.hh>
#include <regex>

int pg_regcomp(regex_t *regex, const char *pattern, int cflags) {
    if (!regex || !pattern) return 1;

    try {
        regex_t std_re(pattern);  // Just to validate
        regex->pattern = pattern;
        regex->error.clear();

        // Stub: create a fake initial and final state
        regex->initial = 0;
        regex->final = 1;
        regex->arcs.push_back({0, 1, 'X'});  // Fake arc for now
        regex->colors = {'X'};

        return 0;  // success
    } catch (const std::regex_error &e) {
        regex->error = e.what();
        return 1;
    }
}

int pg_regerror(int errcode, const regex_t *regex, char *errbuf, size_t errbuf_size) {
    if (!regex || !errbuf) return 0;
    std::string err = regex->error.empty() ? "Unknown error" : regex->error;
    std::snprintf(errbuf, errbuf_size, "%s", err.c_str());
    return err.size();
}

regstate_t pg_reg_getinitialstate(const regex_t *regex) {
    return regex ? regex->initial : -1;
}

regstate_t pg_reg_getfinalstate(const regex_t *regex) {
    return regex ? regex->final : -1;
}

int pg_reg_getnumoutarcs(const regex_t *regex, regstate_t state) {
    if (!regex) return 0;
    int count = 0;
    for (auto &arc : regex->arcs) {
        if (arc.from == state)
            count++;
    }
    return count;
}

const regex_arc_t *pg_reg_getoutarcs(const regex_t *regex, regstate_t state, int *arcs) {
    if (!regex) return nullptr;
    static std::vector<regex_arc_t> filtered;
    filtered.clear();
    for (auto &arc : regex->arcs) {
        if (arc.from == state)
            filtered.push_back(arc);
    }
    *arcs = filtered.size();
    return filtered.data();
}

int pg_reg_getnumcharacters(const regex_t *regex, int arc) {
    if (!regex) return 0;
    return regex->colors.size();
}

const regcolor_t *pg_reg_getcharacters(const regex_t *regex, int arc, int *chars) {
    if (!regex) return nullptr;
    *chars = regex->colors.size();
    return regex->colors.data();
}

int pg_reg_getnumcolors(const regex_t *regex) {
    return regex ? regex->colors.size() : 0;
}

bool pg_reg_colorisbegin(const regex_t *regex, regcolor_t color) {
    return false; // XXX Stubbed for now
}

bool pg_reg_colorisend(const regex_t *regex, regcolor_t color) {
    return false; // XXX Stubbed for now
}
