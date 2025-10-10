#include <pg_ext/float.hh>
#include <pg_ext/memory.hh>
#include <cstdlib>
#include <cmath>

#include <common/logging.hh>

const int extra_float_digits = 1;

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
        std::strncpy(ascii, "NaN", 127);
    	ascii[127] = '\0';
    }
    else if (std::isinf(num))
    {
        if (num < 0)
            std::strncpy(ascii, "-Infinity", 127);
        else
            std::strncpy(ascii, "Infinity", 127);
        ascii[127] = '\0';
    }
    else if (extra_float_digits > 0)
    {
        // Use shortest decimal via ostringstream + max precision
        std::ostringstream oss;
		oss << std::setprecision(std::numeric_limits<double>::max_digits10)
			<< num;

		std::snprintf(ascii, 128, "%s", oss.str().c_str());
    }
    else
    {
        // fixed precision formatting
        std::snprintf(ascii, 128, "%.*g", ndig, num);
    }

    return ascii;
}

static inline double
get_float8_infinity(void)
{
#ifdef INFINITY
	/* C99 standard way */
	return (double) INFINITY;
#else
	/*
	 * On some platforms, HUGE_VAL is an infinity, elsewhere it's just the
	 * largest normal float8.  We assume forcing an overflow will get us a
	 * true infinity.
	 */
	return (float8) (HUGE_VAL * HUGE_VAL);
#endif
}

static inline double
get_float8_nan(void)
{
	/* (float8) NAN doesn't work on some NetBSD/MIPS releases */
#if defined(NAN) && !(defined(__NetBSD__) && defined(__mips__))
	/* C99 standard way */
	return (double) NAN;
#else
	/* Assume we can get a NaN via zero divide */
	return (double) (0.0 / 0.0);
#endif
}

double
float8in_internal(char *num, char **endptr_p,
				  const char *type_name, const char *orig_string,
				  struct Node *escontext)
{
	double		val;
	char	   *endptr;

	/* skip leading whitespace */
	while (*num != '\0' && isspace((unsigned char) *num))
		num++;

	/*
	 * Check for an empty-string input to begin with, to avoid the vagaries of
	 * strtod() on different platforms.
	 */
	if (*num == '\0'){
        LOG_ERROR("invalid input syntax for type %s: \"%s\"",
            type_name, orig_string);
        return 0;
    }

	errno = 0;
	val = strtod(num, &endptr);

	/* did we not see anything that looks like a double? */
	if (endptr == num || errno != 0)
	{
		int			save_errno = errno;

		/*
		 * C99 requires that strtod() accept NaN, [+-]Infinity, and [+-]Inf,
		 * but not all platforms support all of these (and some accept them
		 * but set ERANGE anyway...)  Therefore, we check for these inputs
		 * ourselves if strtod() fails.
		 *
		 * Note: C99 also requires hexadecimal input as well as some extended
		 * forms of NaN, but we consider these forms unportable and don't try
		 * to support them.  You can use 'em if your strtod() takes 'em.
		 */
		if (strncasecmp(num, "NaN", 3) == 0)
		{
			val = get_float8_nan();
			endptr = num + 3;
		}
		else if (strncasecmp(num, "Infinity", 8) == 0)
		{
			val = get_float8_infinity();
			endptr = num + 8;
		}
		else if (strncasecmp(num, "+Infinity", 9) == 0)
		{
			val = get_float8_infinity();
			endptr = num + 9;
		}
		else if (strncasecmp(num, "-Infinity", 9) == 0)
		{
			val = -get_float8_infinity();
			endptr = num + 9;
		}
		else if (strncasecmp(num, "inf", 3) == 0)
		{
			val = get_float8_infinity();
			endptr = num + 3;
		}
		else if (strncasecmp(num, "+inf", 4) == 0)
		{
			val = get_float8_infinity();
			endptr = num + 4;
		}
		else if (strncasecmp(num, "-inf", 4) == 0)
		{
			val = -get_float8_infinity();
			endptr = num + 4;
		}
		else if (save_errno == ERANGE)
		{
			/*
			 * Some platforms return ERANGE for denormalized numbers (those
			 * that are not zero, but are too close to zero to have full
			 * precision).  We'd prefer not to throw error for that, so try to
			 * detect whether it's a "real" out-of-range condition by checking
			 * to see if the result is zero or huge.
			 *
			 * On error, we intentionally complain about double precision not
			 * the given type name, and we print only the part of the string
			 * that is the current number.
			 */
			if (val == 0.0 || val >= HUGE_VAL || val <= -HUGE_VAL)
			{
				char	   *errnumber = pstrdup(num);

				errnumber[endptr - num] = '\0';
				LOG_ERROR("\"%s\" is out of range for type double precision",
					errnumber);
                return 0;
			}
		}
		else
		{
            LOG_ERROR("invalid input syntax for type %s: \"%s\"",
                type_name, orig_string);
            return 0;
        }
	}

	/* skip trailing whitespace */
	while (*endptr != '\0' && isspace((unsigned char) *endptr))
		endptr++;

	/* report stopping point if wanted, else complain if not end of string */
	if (endptr_p)
		*endptr_p = endptr;
	else if (*endptr != '\0'){
        LOG_ERROR("invalid input syntax for type %s: \"%s\"",
                type_name, orig_string);
        return 0;
    }

	return val;
}
