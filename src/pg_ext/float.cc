#include <pg_ext/float.hh>
#include <pg_ext/memory.hh>
#include <cstdlib>

#include <common/logging.hh>

int extra_float_digits = 1;
#define DOUBLE_SHORTEST_DECIMAL_LEN 25

char *
float8out_internal(double num)
{
    // allocate plenty of space for formatted double
    char *ascii = (char *) palloc(128);

    // clamp ndig like Postgres does
    int ndig = std::numeric_limits<double>::digits10 + extra_float_digits;
    if (ndig < 1)
        ndig = 1;
    else if (ndig > std::numeric_limits<double>::digits10 + 3)
        ndig = std::numeric_limits<double>::digits10 + 3;

    if (std::isnan(num))
    {
        strcpy(ascii, "NaN");
    }
    else if (std::isinf(num))
    {
        if (num < 0)
            strcpy(ascii, "-Infinity");
        else
            strcpy(ascii, "Infinity");
    }
    else if (extra_float_digits > 0)
    {
        // Use shortest decimal via ostringstream + max precision
        std::ostringstream oss;
        oss << std::setprecision(std::numeric_limits<double>::max_digits10)
            << num;
        strncpy(ascii, oss.str().c_str(), 127);
        ascii[127] = '\0';
    }
    else
    {
        // fixed precision formatting
        std::snprintf(ascii, 128, "%.*g", ndig, num);
    }

    return ascii;
}

double float8in_internal(const char *numstr) {
    // XXX Stubbed for now
    return 0.0;
}
