#include <pg_ext/date.hh>

#include <fcntl.h>

const char * const months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", nullptr};
const char * const days[] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", nullptr};

const int DateOrder = DATEORDER_MDY;

DateADT
DatumGetDateADT(Datum X)
{
    return (DateADT) DatumGetInt32(X);
}

TimeADT
DatumGetTimeADT(Datum X)
{
	return (TimeADT) DatumGetInt64(X);
}

TimeTzADT *
DatumGetTimeTzADTP(Datum X)
{
	return (TimeTzADT *) DatumGetPointer(X);
}

Timestamp
DatumGetTimestamp(Datum X)
{
	return (Timestamp) DatumGetInt64(X);
}

TimestampTz
DatumGetTimestampTz(Datum X)
{
	return (TimestampTz) DatumGetInt64(X);
}

void
EncodeDateOnly(struct pg_tm *tm, int style, char *str)
{
	assert(tm->tm_mon >= 1 && tm->tm_mon <= MONTHS_PER_YEAR);

	switch (style)
	{
		case USE_ISO_DATES:
		case USE_XSD_DATES:
			/* compatible with ISO date formats */
			str = pg_ultostr_zeropad(str,
									 (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1), 4);
			*str++ = '-';
			str = pg_ultostr_zeropad(str, tm->tm_mon, 2);
			*str++ = '-';
			str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
			break;

		case USE_SQL_DATES:
			/* compatible with Oracle/Ingres date formats */
			if (DateOrder == DATEORDER_DMY)
			{
				str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
				*str++ = '/';
				str = pg_ultostr_zeropad(str, tm->tm_mon, 2);
			}
			else
			{
				str = pg_ultostr_zeropad(str, tm->tm_mon, 2);
				*str++ = '/';
				str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
			}
			*str++ = '/';
			str = pg_ultostr_zeropad(str,
									 (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1), 4);
			break;

		case USE_GERMAN_DATES:
			/* German-style date format */
			str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
			*str++ = '.';
			str = pg_ultostr_zeropad(str, tm->tm_mon, 2);
			*str++ = '.';
			str = pg_ultostr_zeropad(str,
									 (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1), 4);
			break;

		case USE_POSTGRES_DATES:
		default:
			/* traditional date-only style for Postgres */
			if (DateOrder == DATEORDER_DMY)
			{
				str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
				*str++ = '-';
				str = pg_ultostr_zeropad(str, tm->tm_mon, 2);
			}
			else
			{
				str = pg_ultostr_zeropad(str, tm->tm_mon, 2);
				*str++ = '-';
				str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
			}
			*str++ = '-';
			str = pg_ultostr_zeropad(str,
									 (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1), 4);
			break;
	}

	if (tm->tm_year <= 0)
	{
		memcpy(str, " BC", 3);	/* Don't copy NUL */
		str += 3;
	}
	*str = '\0';
}

static int32_t
detzcode(const char *const codep)
{
	int32_t		result = 0;
	int32_t		one = 1;
	int32_t		halfmaxval = one << (32 - 2);
	int32_t		maxval = halfmaxval - 1 + halfmaxval;
	int32_t		minval = -1 - maxval;

	result = codep[0] & 0x7f;
	for (int i = 1; i < 4; ++i)
		result = (result << 8) | (codep[i] & 0xff);

	if (codep[0] & 0x80)
	{
		/*
		 * Do two's-complement negation even on non-two's-complement machines.
		 * If the result would be minval - 1, return minval.
		 */
		result -= !TWOS_COMPLEMENT(int32_t) && result != 0;
		result += minval;
	}
	return result;
}

static int64_t
detzcode64(const char *const codep)
{
	uint64_t	result = 0;
	int64_t		one = 1;
	int64_t		halfmaxval = one << (64 - 2);
	int64_t		maxval = halfmaxval - 1 + halfmaxval;
	int64_t		minval = -TWOS_COMPLEMENT(int64_t) - maxval;

	result = codep[0] & 0x7f;
	for (int i = 1; i < 8; ++i)
		result = (result << 8) | (codep[i] & 0xff);

	if (codep[0] & 0x80)
	{
		/*
		 * Do two's-complement negation even on non-two's-complement machines.
		 * If the result would be minval - 1, return minval.
		 */
		result -= !TWOS_COMPLEMENT(int64_t) && result != 0;
		result += minval;
	}
	return result;
}

static const char *
pg_TZDIR(void)
{
	/* we're configured to use system's timezone database */
	// XXX Need to fix
	return "";
}

int
pg_open_tzfile(const char *name, char *canonname)
{
    char fullname[MAXPGPATH];

    if (canonname)
        strlcpy(canonname, name, TZ_STRLEN_MAX + 1);

    strlcpy(fullname, pg_TZDIR(), sizeof(fullname));

    if (strlcat(fullname, "/", sizeof(fullname)) >= sizeof(fullname))
        return -1; // overflow
    if (strlcat(fullname, name, sizeof(fullname)) >= sizeof(fullname))
        return -1; // overflow

    return open(fullname, O_RDONLY | PG_BINARY, 0);
}

static const char *
getzname(const char *strp)
{
	char		c = 0;

	while ((c = *strp) != '\0' && !is_digit(c) && c != ',' && c != '-' &&
		   c != '+')
		++strp;
	return strp;
}

static const char *
getqzname(const char *strp, const int delim)
{
	int			c	= 0;

	while ((c = *strp) != '\0' && c != delim)
		++strp;
	return strp;
}

static const char *
getnum(const char *strp, int *const nump, const int min, const int max)
{
	char		c	= 0;
	int			num = 0;

	if (strp == nullptr || !is_digit(c = *strp))
		return nullptr;
	num = 0;
	do
	{
		num = num * 10 + (c - '0');
		if (num > max)
			return nullptr;		/* illegal value */
		c = *++strp;
	} while (is_digit(c));
	if (num < min)
		return nullptr;			/* illegal value */
	*nump = num;
	return strp;
}

static const char *
getsecs(const char *strp, int32_t *const secsp)
{
	int			num = 0;

	/*
	 * 'HOURSPERDAY * DAYSPERWEEK - 1' allows quasi-Posix rules like
	 * "M10.4.6/26", which does not conform to Posix, but which specifies the
	 * equivalent of "02:00 on the first Sunday on or after 23 Oct".
	 */
	strp = getnum(strp, &num, 0, HOURSPERDAY * DAYSPERWEEK - 1);
	if (strp == nullptr)
		return nullptr;
	*secsp = num * (int32_t) SECSPERHOUR;
	if (*strp == ':')
	{
		++strp;
		strp = getnum(strp, &num, 0, MINSPERHOUR - 1);
		if (strp == nullptr)
			return nullptr;
		*secsp += num * SECSPERMIN;
		if (*strp == ':')
		{
			++strp;
			/* 'SECSPERMIN' allows for leap seconds.  */
			strp = getnum(strp, &num, 0, SECSPERMIN);
			if (strp == nullptr)
				return nullptr;
			*secsp += num;
		}
	}
	return strp;
}

static const char *
getoffset(const char *strp, int32_t *const offsetp)
{
	bool		neg = false;

	if (*strp == '-')
	{
		neg = true;
		++strp;
	}
	else if (*strp == '+')
		++strp;
	strp = getsecs(strp, offsetp);
	if (strp == nullptr)
		return nullptr;			/* illegal time */
	if (neg)
		*offsetp = -*offsetp;
	return strp;
}

static void
init_ttinfo(struct ttinfo *s, int32_t utoff, bool isdst, int desigidx)
{
	s->tt_utoff = utoff;
	s->tt_isdst = isdst;
	s->tt_desigidx = desigidx;
	s->tt_ttisstd = false;
	s->tt_ttisut = false;
}

static bool
increment_overflow_time(pg_time_t *tp, int32_t j)
{
	/*----------
	 * This is like
	 * 'if (! (TIME_T_MIN <= *tp + j && *tp + j <= TIME_T_MAX)) ...',
	 * except that it does the right thing even if *tp + j would overflow.
	 *----------
	 */
	if (!(j < 0
		  ? (TYPE_SIGNED(pg_time_t) ? TIME_T_MIN - j <= *tp : -1 - j < *tp)
		  : *tp <= TIME_T_MAX - j))
		return true;
	*tp += j;
	return false;
}

static const char *
getrule(const char *strp, struct rule *const rulep)
{
	if (*strp == 'J')
	{
		/*
		 * Julian day.
		 */
		rulep->r_type = r_type::JULIAN_DAY;
		++strp;
		strp = getnum(strp, &rulep->r_day, 1, DAYSPERNYEAR);
	}
	else if (*strp == 'M')
	{
		/*
		 * Month, week, day.
		 */
		rulep->r_type = r_type::MONTH_NTH_DAY_OF_WEEK;
		++strp;
		strp = getnum(strp, &rulep->r_mon, 1, MONSPERYEAR);
		if (strp == nullptr)
			return nullptr;
		if (*strp++ != '.')
			return nullptr;
		strp = getnum(strp, &rulep->r_week, 1, 5);
		if (strp == nullptr)
			return nullptr;
		if (*strp++ != '.')
			return nullptr;
		strp = getnum(strp, &rulep->r_day, 0, DAYSPERWEEK - 1);
	}
	else if (is_digit(*strp))
	{
		/*
		 * Day of year.
		 */
		rulep->r_type = r_type::DAY_OF_YEAR;
		strp = getnum(strp, &rulep->r_day, 0, DAYSPERLYEAR - 1);
	}
	else
		return nullptr;			/* invalid format */
	if (strp == nullptr)
		return nullptr;
	if (*strp == '/')
	{
		/*
		 * Time specified.
		 */
		++strp;
		strp = getoffset(strp, &rulep->r_time);
	}
	else
		rulep->r_time = 2 * SECSPERHOUR;	/* default = 2:00:00 */
	return strp;
}

static int32_t
transtime(const int year, const struct rule *const rulep,
		  const int32_t offset)
{
	bool		leapyear = false;
	int32_t		value = 0;
	int			d = 0;
	int			m1 = 0;
	int			yy0 = 0;
	int			yy1 = 0;
	int			yy2 = 0;
	int			dow = 0;

	INITIALIZE(value);
	leapyear = isleap(year);
	switch (rulep->r_type)
	{

		case r_type::JULIAN_DAY:

			/*
			 * Jn - Julian day, 1 == January 1, 60 == March 1 even in leap
			 * years. In non-leap years, or if the day number is 59 or less,
			 * just add SECSPERDAY times the day number-1 to the time of
			 * January 1, midnight, to get the day.
			 */
			value = (rulep->r_day - 1) * SECSPERDAY;
			if (leapyear && rulep->r_day >= 60)
				value += SECSPERDAY;
			break;

		case r_type::DAY_OF_YEAR:

			/*
			 * n - day of year. Just add SECSPERDAY times the day number to
			 * the time of January 1, midnight, to get the day.
			 */
			value = rulep->r_day * SECSPERDAY;
			break;

		case r_type::MONTH_NTH_DAY_OF_WEEK:

			/*
			 * Mm.n.d - nth "dth day" of month m.
			 */

			/*
			 * Use Zeller's Congruence to get day-of-week of first day of
			 * month.
			 */
			m1 = (rulep->r_mon + 9) % 12 + 1;
			yy0 = (rulep->r_mon <= 2) ? (year - 1) : year;
			yy1 = yy0 / 100;
			yy2 = yy0 % 100;
			dow = ((26 * m1 - 2) / 10 +
				   1 + yy2 + yy2 / 4 + yy1 / 4 - 2 * yy1) % 7;
			if (dow < 0)
				dow += DAYSPERWEEK;

			/*
			 * "dow" is the day-of-week of the first day of the month. Get the
			 * day-of-month (zero-origin) of the first "dow" day of the month.
			 */
			d = rulep->r_day - dow;
			if (d < 0)
				d += DAYSPERWEEK;
			for (i = 1; i < rulep->r_week; ++i)
			{
				if (d + DAYSPERWEEK >=
					mon_lengths[(int) leapyear][rulep->r_mon - 1])
					break;
				d += DAYSPERWEEK;
			}

			/*
			 * "d" is the day-of-month (zero-origin) of the day we want.
			 */
			value = d * SECSPERDAY;
			for (int i = 0; i < rulep->r_mon - 1; ++i)
				value += mon_lengths[(int) leapyear][i] * SECSPERDAY;
			break;
	}

	/*
	 * "value" is the year-relative time of 00:00:00 UT on the day in
	 * question. To get the year-relative time of the specified local time on
	 * that day, add the transition time and the current offset from UT.
	 */
	return value + rulep->r_time + offset;
}

bool
tzparse(const char *name, struct state *sp, bool lastditch)
{
	const char *stdname = nullptr;
	const char *dstname = nullptr;
	size_t		stdlen = 0;
	size_t		dstlen = 0;
	size_t		charcnt = 0;
	int32_t		stdoffset = 0;
	int32_t		dstoffset = 0;
	char	   *cp = nullptr;
	bool		load_ok = false;

	stdname = name;
    if (lastditch)
    {
        /* Unlike IANA, don't assume name is exactly "GMT" */
        if (!name || strnlen(name, TZ_STRLEN_MAX) == 0)
            return false;
        stdlen = strnlen(name, TZ_STRLEN_MAX);	/* length of standard zone name */
        name += stdlen;
        stdoffset = 0;
    }
	else
	{
		if (*name == '<')
		{
			name++;
			stdname = name;
			name = getqzname(name, '>');
			if (*name != '>')
				return false;
			stdlen = name - stdname;
			name++;
		}
		else
		{
			name = getzname(name);
			stdlen = name - stdname;
		}
		if (*name == '\0')		/* we allow empty STD abbrev, unlike IANA */
			return false;
		name = getoffset(name, &stdoffset);
		if (name == nullptr)
			return false;
	}
	charcnt = stdlen + 1;
	if (sizeof sp->chars < charcnt)
		return false;

	/*
	 * The IANA code always tries to tzload(TZDEFRULES) here.  We do not want
	 * to do that; it would be bad news in the lastditch case, where we can't
	 * assume pg_open_tzfile() is sane yet.  Moreover, if we did load it and
	 * it contains leap-second-dependent info, that would cause problems too.
	 * Finally, IANA has deprecated the TZDEFRULES feature, so it presumably
	 * will die at some point.  Desupporting it now seems like good
	 * future-proofing.
	 */
	load_ok = false;
	sp->goback = sp->goahead = false;	/* simulate failed tzload() */
	sp->leapcnt = 0;			/* intentionally assume no leap seconds */

	if (*name != '\0')
	{
		if (*name == '<')
		{
			dstname = ++name;
			name = getqzname(name, '>');
			if (*name != '>')
				return false;
			dstlen = name - dstname;
			name++;
		}
		else
		{
			dstname = name;
			name = getzname(name);
			dstlen = name - dstname;	/* length of DST abbr. */
		}
		if (!dstlen)
			return false;
		charcnt += dstlen + 1;
		if (sizeof sp->chars < charcnt)
			return false;
		if (*name != '\0' && *name != ',' && *name != ';')
		{
			name = getoffset(name, &dstoffset);
			if (name == nullptr)
				return false;
		}
		else
			dstoffset = stdoffset - SECSPERHOUR;
		if (*name == '\0' && !load_ok)
			name = TZDEFRULESTRING;
		if (*name == ',' || *name == ';')
		{
			struct rule start;
			struct rule end;
			int			year;
			int			yearlim;
			int			timecnt;
			pg_time_t	janfirst;
			int32_t		janoffset = 0;
			int			yearbeg;

			++name;
			if ((name = getrule(name, &start)) == nullptr)
				return false;
			if (*name++ != ',')
				return false;
			if ((name = getrule(name, &end)) == nullptr)
				return false;
			if (*name != '\0')
				return false;
			sp->typecnt = 2;	/* standard time and DST */

			/*
			 * Two transitions per year, from EPOCH_YEAR forward.
			 */
			init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
			init_ttinfo(&sp->ttis[1], -dstoffset, true, stdlen + 1);
			sp->defaulttype = 0;
			timecnt = 0;
			janfirst = 0;
			yearbeg = EPOCH_YEAR;

			do
			{
				int32_t		yearsecs
				= year_lengths[isleap(yearbeg - 1)] * SECSPERDAY;

				yearbeg--;
				if (increment_overflow_time(&janfirst, -yearsecs))
				{
					janoffset = -yearsecs;
					break;
				}
			} while (EPOCH_YEAR - YEARSPERREPEAT / 2 < yearbeg);

			yearlim = yearbeg + YEARSPERREPEAT + 1;
			for (year = yearbeg; year < yearlim; year++)
			{
				int32_t			starttime = transtime(year, &start, stdoffset),
									endtime = transtime(year, &end, dstoffset);
				int32_t			yearsecs = (year_lengths[isleap(year)]
										* SECSPERDAY);
				bool		reversed = endtime < starttime;

				if (reversed)
				{
					int32_t		swap = starttime;

					starttime = endtime;
					endtime = swap;
				}
				if (reversed
					|| (starttime < endtime
						&& (endtime - starttime
							< (yearsecs
							   + (stdoffset - dstoffset)))))
				{
					if (TZ_MAX_TIMES - 2 < timecnt)
						break;
					sp->ats[timecnt] = janfirst;
					if (!increment_overflow_time
						(&sp->ats[timecnt],
						 janoffset + starttime))
						sp->types[timecnt++] = !reversed;
					sp->ats[timecnt] = janfirst;
					if (!increment_overflow_time
						(&sp->ats[timecnt],
						 janoffset + endtime))
					{
						sp->types[timecnt++] = reversed;
						yearlim = year + YEARSPERREPEAT + 1;
					}
				}
				if (increment_overflow_time
					(&janfirst, janoffset + yearsecs))
					break;
				janoffset = 0;
			}
			sp->timecnt = timecnt;
			if (!timecnt)
			{
				sp->ttis[0] = sp->ttis[1];
				sp->typecnt = 1;	/* Perpetual DST.  */
			}
			else if (YEARSPERREPEAT < year - yearbeg)
				sp->goback = sp->goahead = true;
		}
		else
		{
			int32_t		theirstdoffset;
			int32_t		theirdstoffset;
			int32_t		theiroffset;
			bool		isdst;
			int			i;
			int			j;

			if (*name != '\0')
				return false;

			/*
			 * Initial values of theirstdoffset and theirdstoffset.
			 */
			theirstdoffset = 0;
			for (i = 0; i < sp->timecnt; ++i)
			{
				j = sp->types[i];
				if (!sp->ttis[j].tt_isdst)
				{
					theirstdoffset =
						-sp->ttis[j].tt_utoff;
					break;
				}
			}
			theirdstoffset = 0;
			for (i = 0; i < sp->timecnt; ++i)
			{
				j = sp->types[i];
				if (sp->ttis[j].tt_isdst)
				{
					theirdstoffset =
						-sp->ttis[j].tt_utoff;
					break;
				}
			}

			/*
			 * Initially we're assumed to be in standard time.
			 */
			isdst = false;
			theiroffset = theirstdoffset;

			/*
			 * Now juggle transition times and types tracking offsets as you
			 * do.
			 */
			for (i = 0; i < sp->timecnt; ++i)
			{
				j = sp->types[i];
				sp->types[i] = sp->ttis[j].tt_isdst;
				if (sp->ttis[j].tt_ttisut)
				{
					/* No adjustment to transition time */
				}
				else
				{
					/*
					 * If daylight saving time is in effect, and the
					 * transition time was not specified as standard time, add
					 * the daylight saving time offset to the transition time;
					 * otherwise, add the standard time offset to the
					 * transition time.
					 */
					/*
					 * Transitions from DST to DDST will effectively disappear
					 * since POSIX provides for only one DST offset.
					 */
					if (isdst && !sp->ttis[j].tt_ttisstd)
					{
						sp->ats[i] += dstoffset -
							theirdstoffset;
					}
					else
					{
						sp->ats[i] += stdoffset -
							theirstdoffset;
					}
				}
				theiroffset = -sp->ttis[j].tt_utoff;
				if (sp->ttis[j].tt_isdst)
					theirdstoffset = theiroffset;
				else
					theirstdoffset = theiroffset;
			}

			/*
			 * Finally, fill in ttis.
			 */
			init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
			init_ttinfo(&sp->ttis[1], -dstoffset, true, stdlen + 1);
			sp->typecnt = 2;
			sp->defaulttype = 0;
		}
	}
	else
	{
		dstlen = 0;
		sp->typecnt = 1;		/* only standard time */
		sp->timecnt = 0;
		init_ttinfo(&sp->ttis[0], -stdoffset, false, 0);
		sp->defaulttype = 0;
	}
	sp->charcnt = charcnt;
	cp = sp->chars;
	memcpy(cp, stdname, stdlen);
	cp += stdlen;
	*cp++ = '\0';
	if (dstlen != 0)
	{
		memcpy(cp, dstname, dstlen);
		*(cp + dstlen) = '\0';
	}
	return true;
}

static bool
typesequiv(const struct state *sp, int a, int b)
{
	bool		result = false;

	if (sp == nullptr ||
		a < 0 || a >= sp->typecnt ||
		b < 0 || b >= sp->typecnt)
		result = false;
	else
	{
		const struct ttinfo *ap = &sp->ttis[a];
		const struct ttinfo *bp = &sp->ttis[b];

		result = (ap->tt_utoff == bp->tt_utoff
				  && ap->tt_isdst == bp->tt_isdst
				  && ap->tt_ttisstd == bp->tt_ttisstd
				  && ap->tt_ttisut == bp->tt_ttisut
				  && (strcmp(&sp->chars[ap->tt_desigidx],
							 &sp->chars[bp->tt_desigidx])
					  == 0));
	}
	return result;
}

static int64_t
leapcorr(struct state const *sp, pg_time_t t)
{
	struct lsinfo const *lp = nullptr;
	int			i = sp->leapcnt;
	while (--i >= 0)
	{
		lp = &sp->lsis[i];
		if (t >= lp->ls_trans)
			return lp->ls_corr;
	}
	return 0;
}

static bool
differ_by_repeat(const pg_time_t t1, const pg_time_t t0)
{
	if (TYPE_BIT(pg_time_t) - TYPE_SIGNED(pg_time_t) < SECSPERREPEAT_BITS)
		return 0;
	return t1 - t0 == SECSPERREPEAT;
}

/* Load tz data from the file named NAME into *SP.  Read extended
 * format if DOEXTEND.  Use *LSP for temporary storage.  Return 0 on
 * success, an errno value on failure.
 * PG: If "canonname" is not nullptr, then on success the canonical spelling of
 * given name is stored there (the buffer must be > TZ_STRLEN_MAX bytes!).
 */
 static int
 tzloadbody(char const *name, char *canonname, struct state *sp, bool doextend,
            union local_storage *lsp)
 {
     int			fid = 0;
     ssize_t		nread = 0;
     union input_buffer *up = &lsp->u.u;
     int			tzheadsize = sizeof(struct tzhead);

     sp->goback = sp->goahead = false;

     if (!name)
     {
         name = TZDEFAULT;
         if (!name)
             return EINVAL;
     }

     if (name[0] == ':')
         ++name;

     fid = pg_open_tzfile(name, canonname);
     if (fid < 0)
         return ENOENT;			/* pg_open_tzfile may not set errno */

     nread = read(fid, up->buf, sizeof up->buf);
     if (nread < tzheadsize)
     {
         int			err = nread < 0 ? errno : EINVAL;

         close(fid);
         return err;
     }
     if (close(fid) < 0)
         return errno;
     for (int stored = 4; stored <= 8; stored *= 2)
     {
         int32_t		ttisstdcnt = detzcode(up->tzhead.tzh_ttisstdcnt);
         int32_t		ttisutcnt = detzcode(up->tzhead.tzh_ttisutcnt);
         int64_t		prevtr = 0;
         int32_t		prevcorr = 0;
         int32_t		leapcnt = detzcode(up->tzhead.tzh_leapcnt);
         int32_t		timecnt = detzcode(up->tzhead.tzh_timecnt);
         int32_t		typecnt = detzcode(up->tzhead.tzh_typecnt);
         int32_t		charcnt = detzcode(up->tzhead.tzh_charcnt);
         char const *p = up->buf + tzheadsize;

         /*
          * Although tzfile(5) currently requires typecnt to be nonzero,
          * support future formats that may allow zero typecnt in files that
          * have a TZ string and no transitions.
          */
         if (!(0 <= leapcnt && leapcnt < TZ_MAX_LEAPS
               && 0 <= typecnt && typecnt < TZ_MAX_TYPES
               && 0 <= timecnt && timecnt < TZ_MAX_TIMES
               && 0 <= charcnt && charcnt < TZ_MAX_CHARS
               && (ttisstdcnt == typecnt || ttisstdcnt == 0)
               && (ttisutcnt == typecnt || ttisutcnt == 0)))
             return EINVAL;
         if (nread
             < (tzheadsize		/* struct tzhead */
                + timecnt * stored	/* ats */
                + timecnt		/* types */
                + typecnt * 6	/* ttinfos */
                + charcnt		/* chars */
                + leapcnt * (stored + 4) /* lsinfos */
                + ttisstdcnt		/* ttisstds */
                + ttisutcnt))	/* ttisuts */
             return EINVAL;
         sp->leapcnt = leapcnt;
         sp->timecnt = timecnt;
         sp->typecnt = typecnt;
         sp->charcnt = charcnt;

         /*
          * Read transitions, discarding those out of pg_time_t range. But
          * pretend the last transition before TIME_T_MIN occurred at
          * TIME_T_MIN.
          */
         timecnt = 0;
         for (i = 0; i < sp->timecnt; ++i)
         {
             int64_t		at
             = stored == 4 ? detzcode(p) : detzcode64(p);

             sp->types[i] = at <= TIME_T_MAX;
             if (sp->types[i])
             {
                 pg_time_t	attime
                 = ((TYPE_SIGNED(pg_time_t) ? at < TIME_T_MIN : at < 0)
                    ? TIME_T_MIN : at);

                 if (timecnt && attime <= sp->ats[timecnt - 1])
                 {
                     if (attime < sp->ats[timecnt - 1])
                         return EINVAL;
                     sp->types[i - 1] = 0;
                     timecnt--;
                 }
                 sp->ats[timecnt++] = attime;
             }
             p += stored;
         }

         timecnt = 0;
         for (i = 0; i < sp->timecnt; ++i)
         {
             unsigned char typ = *p++;

             if (sp->typecnt <= typ)
                 return EINVAL;
             if (sp->types[i])
                 sp->types[timecnt++] = typ;
         }
         sp->timecnt = timecnt;
         for (i = 0; i < sp->typecnt; ++i)
         {
             struct ttinfo *ttisp;
             unsigned char isdst,
                         desigidx;

             ttisp = &sp->ttis[i];
             ttisp->tt_utoff = detzcode(p);
             p += 4;
             isdst = *p++;
             if (!(isdst < 2))
                 return EINVAL;
             ttisp->tt_isdst = isdst;
             desigidx = *p++;
             if (!(desigidx < sp->charcnt))
                 return EINVAL;
             ttisp->tt_desigidx = desigidx;
         }
         for (i = 0; i < sp->charcnt; ++i)
             sp->chars[i] = *p++;
         sp->chars[i] = '\0';	/* ensure '\0' at end */

         /* Read leap seconds, discarding those out of pg_time_t range.  */
         leapcnt = 0;
         for (i = 0; i < sp->leapcnt; ++i)
         {
             int64_t		tr = stored == 4 ? detzcode(p) : detzcode64(p);
             int32_t		corr = detzcode(p + stored);

             p += stored + 4;
             /* Leap seconds cannot occur before the Epoch.  */
             if (tr < 0)
                 return EINVAL;
             if (tr <= TIME_T_MAX)
             {
                 /*
                  * Leap seconds cannot occur more than once per UTC month, and
                  * UTC months are at least 28 days long (minus 1 second for a
                  * negative leap second).  Each leap second's correction must
                  * differ from the previous one's by 1 second.
                  */
                 if (tr - prevtr < 28 * SECSPERDAY - 1
                     || (corr != prevcorr - 1 && corr != prevcorr + 1))
                     return EINVAL;
                 sp->lsis[leapcnt].ls_trans = prevtr = tr;
                 sp->lsis[leapcnt].ls_corr = prevcorr = corr;
                 leapcnt++;
             }
         }
         sp->leapcnt = leapcnt;

         for (i = 0; i < sp->typecnt; ++i)
         {
             struct ttinfo *ttisp;

             ttisp = &sp->ttis[i];
             if (ttisstdcnt == 0)
                 ttisp->tt_ttisstd = false;
             else
             {
                 if (*p != true && *p != false)
                     return EINVAL;
                 ttisp->tt_ttisstd = *p++;
             }
         }
         for (int i = 0; i < sp->typecnt; ++i)
         {
             struct ttinfo *ttisp;

             ttisp = &sp->ttis[i];
             if (ttisutcnt == 0)
                 ttisp->tt_ttisut = false;
             else
             {
                 if (*p != true && *p != false)
                     return EINVAL;
                 ttisp->tt_ttisut = *p++;
             }
         }

         /*
          * If this is an old file, we're done.
          */
         if (up->tzhead.tzh_version[0] == '\0')
             break;
         nread -= p - up->buf;
         memmove(up->buf, p, nread);
     }
     if (doextend && nread > 2 &&
         up->buf[0] == '\n' && up->buf[nread - 1] == '\n' &&
         sp->typecnt + 2 <= TZ_MAX_TYPES)
     {
         struct state *ts = &lsp->u.st;

         up->buf[nread - 1] = '\0';
         if (tzparse(&up->buf[1], ts, false))
         {
             /*
              * Attempt to reuse existing abbreviations. Without this,
              * America/Anchorage would be right on the edge after 2037 when
              * TZ_MAX_CHARS is 50, as sp->charcnt equals 40 (for LMT AST AWT
              * APT AHST AHDT YST AKDT AKST) and ts->charcnt equals 10 (for
              * AKST AKDT).  Reusing means sp->charcnt can stay 40 in this
              * example.
              */
             int			gotabbr = 0;
             int			charcnt = sp->charcnt;

             for (i = 0; i < ts->typecnt; i++)
             {
                 char	   *tsabbr = ts->chars + ts->ttis[i].tt_desigidx;
                 int			j;

                 for (j = 0; j < charcnt; j++)
                     if (strcmp(sp->chars + j, tsabbr) == 0)
                     {
                         ts->ttis[i].tt_desigidx = j;
                         gotabbr++;
                         break;
                     }
                 if (!(j < charcnt))
                 {
                     size_t		tsabbrlen = strnlen(tsabbr, TZ_MAX_CHARS - j);

                     if (j + tsabbrlen < TZ_MAX_CHARS)
                     {
						snprintf(sp->chars + j, TZ_MAX_CHARS - j, "%s", tsabbr);
						charcnt = j + tsabbrlen + 1;
						ts->ttis[i].tt_desigidx = j;
						gotabbr++;
                     }
                 }
             }
             if (gotabbr == ts->typecnt)
             {
                 sp->charcnt = charcnt;

                 /*
                  * Ignore any trailing, no-op transitions generated by zic as
                  * they don't help here and can run afoul of bugs in zic 2016j
                  * or earlier.
                  */
                 while (1 < sp->timecnt
                        && (sp->types[sp->timecnt - 1]
                            == sp->types[sp->timecnt - 2]))
                     sp->timecnt--;

                 for (i = 0; i < ts->timecnt; i++)
                     if (sp->timecnt == 0
                         || (sp->ats[sp->timecnt - 1]
                             < ts->ats[i] + leapcorr(sp, ts->ats[i])))
                         break;
                 while (i < ts->timecnt
                        && sp->timecnt < TZ_MAX_TIMES)
                 {
                     sp->ats[sp->timecnt]
                         = ts->ats[i] + leapcorr(sp, ts->ats[i]);
                     sp->types[sp->timecnt] = (sp->typecnt
                                               + ts->types[i]);
                     sp->timecnt++;
                     i++;
                 }
                 for (i = 0; i < ts->typecnt; i++)
                     sp->ttis[sp->typecnt++] = ts->ttis[i];
             }
         }
     }
     if (sp->typecnt == 0)
         return EINVAL;
     if (sp->timecnt > 1)
     {
         for (i = 1; i < sp->timecnt; ++i)
             if (typesequiv(sp, sp->types[i], sp->types[0]) &&
                 differ_by_repeat(sp->ats[i], sp->ats[0]))
             {
                 sp->goback = true;
                 break;
             }
         for (i = sp->timecnt - 2; i >= 0; --i)
             if (typesequiv(sp, sp->types[sp->timecnt - 1],
                            sp->types[i]) &&
                 differ_by_repeat(sp->ats[sp->timecnt - 1],
                                  sp->ats[i]))
             {
                 sp->goahead = true;
                 break;
             }
     }

     /*
      * Infer sp->defaulttype from the data.  Although this default type is
      * always zero for data from recent tzdb releases, things are trickier for
      * data from tzdb 2018e or earlier.
      *
      * The first set of heuristics work around bugs in 32-bit data generated
      * by tzdb 2013c or earlier.  The workaround is for zones like
      * Australia/Macquarie where timestamps before the first transition have a
      * time type that is not the earliest standard-time type.  See:
      * https://mm.icann.org/pipermail/tz/2013-May/019368.html
      */

     /*
      * If type 0 is unused in transitions, it's the type to use for early
      * times.
      */
     for (i = 0; i < sp->timecnt; ++i)
         if (sp->types[i] == 0)
             break;
     i = i < sp->timecnt ? -1 : 0;

     /*
      * Absent the above, if there are transition times and the first
      * transition is to a daylight time find the standard type less than and
      * closest to the type of the first transition.
      */
     if (i < 0 && sp->timecnt > 0 && sp->ttis[sp->types[0]].tt_isdst)
     {
         i = sp->types[0];
         while (--i >= 0)
             if (!sp->ttis[i].tt_isdst)
                 break;
     }

     /*
      * The next heuristics are for data generated by tzdb 2018e or earlier,
      * for zones like EST5EDT where the first transition is to DST.
      */

     /*
      * If no result yet, find the first standard type. If there is none, punt
      * to type zero.
      */
     if (i < 0)
     {
         i = 0;
         while (sp->ttis[i].tt_isdst)
             if (++i >= sp->typecnt)
             {
                 i = 0;
                 break;
             }
     }

     /*
      * A simple 'sp->defaulttype = 0;' would suffice here if we didn't have to
      * worry about 2018e-or-earlier data.  Even simpler would be to remove the
      * defaulttype member and just use 0 in its place.
      */
     sp->defaulttype = i;

     return 0;
 }

 /* Load tz data from the file named NAME into *SP.  Read extended
  * format if DOEXTEND.  Return 0 on success, an errno value on failure.
  * PG: If "canonname" is not nullptr, then on success the canonical spelling of
  * given name is stored there (the buffer must be > TZ_STRLEN_MAX bytes!).
  */
 int
 tzload(const char *name, char *canonname, struct state *sp, bool doextend)
 {
     auto *lsp = (union local_storage *) palloc(sizeof(union local_storage));

     if (!lsp)
         return -1;
     else
     {
         int err = tzloadbody(name, canonname, sp, doextend, lsp);

         pfree(lsp);
         return err;
     }
 }

static void
gmtload(struct state *const sp)
{
	if (tzload(gmt, nullptr, sp, true) != 0)
		tzparse(gmt, sp, true);
}

static bool
increment_overflow(int *ip, int j)
{
	int const	i = *ip;

	/*----------
	 * If i >= 0 there can only be overflow if i + j > INT_MAX
	 * or if j > INT_MAX - i; given i >= 0, INT_MAX - i cannot overflow.
	 * If i < 0 there can only be overflow if i + j < INT_MIN
	 * or if j < INT_MIN - i; given i < 0, INT_MIN - i cannot overflow.
	 *----------
	 */
	if ((i >= 0) ? (j > INT_MAX - i) : (j < INT_MIN - i))
		return true;
	*ip += j;
	return false;
}

static int
leaps_thru_end_of_nonneg(int y)
{
	return y / 4 - y / 100 + y / 400;
}

static int
leaps_thru_end_of(const int y)
{
	return (y < 0
			? -1 - leaps_thru_end_of_nonneg(-1 - y)
			: leaps_thru_end_of_nonneg(y));
}

static struct pg_tm *
timesub(const pg_time_t *timep, int32_t offset,
		const struct state *sp, struct pg_tm *tmp)
{
	const struct lsinfo *lp;
	pg_time_t	tdays = 0;
	int			idays = 0;			/* unsigned would be so 2003 */
	int64_t		rem = 0;
	int			y = 0;
	const int  *ip = nullptr;
	int64_t		corr = 0;
	bool		hit = false;
	int			i = 0;

	if (sp)
		i = sp->leapcnt;
	while (--i >= 0)
	{
		{
			lp = &sp->lsis[i];
			if (*timep >= lp->ls_trans)
			{
				corr = lp->ls_corr;
				hit = (*timep == lp->ls_trans
					   && (i == 0 ? 0 : lp[-1].ls_corr) < corr);
				break;
			}
		}
	}
	y = EPOCH_YEAR;
	tdays = *timep / SECSPERDAY;
	rem = *timep % SECSPERDAY;
	while (tdays < 0 || tdays >= year_lengths[isleap(y)])
	{
		int			newy = 0;
		pg_time_t	tdelta = 0;
		int			idelta = 0;
		int			leapdays = 0;

		tdelta = tdays / DAYSPERLYEAR;
		if (!((!TYPE_SIGNED(pg_time_t) || INT_MIN <= tdelta)
			  && tdelta <= INT_MAX))
			goto out_of_range;
		idelta = tdelta;
		if (idelta == 0)
			idelta = (tdays < 0) ? -1 : 1;
		newy = y;
		if (increment_overflow(&newy, idelta))
			goto out_of_range;
		leapdays = leaps_thru_end_of(newy - 1) -
			leaps_thru_end_of(y - 1);
		tdays -= ((pg_time_t) newy - y) * DAYSPERNYEAR;
		tdays -= leapdays;
		y = newy;
	}

	/*
	 * Given the range, we can now fearlessly cast...
	 */
	idays = tdays;
	rem += offset - corr;
	while (rem < 0)
	{
		rem += SECSPERDAY;
		--idays;
	}
	while (rem >= SECSPERDAY)
	{
		rem -= SECSPERDAY;
		++idays;
	}
	while (idays < 0)
	{
		if (increment_overflow(&y, -1))
			goto out_of_range;
		idays += year_lengths[isleap(y)];
	}
	while (idays >= year_lengths[isleap(y)])
	{
		idays -= year_lengths[isleap(y)];
		if (increment_overflow(&y, 1))
			goto out_of_range;
	}
	tmp->tm_year = y;
	if (increment_overflow(&tmp->tm_year, -TM_YEAR_BASE))
		goto out_of_range;
	tmp->tm_yday = idays;

	/*
	 * The "extra" mods below avoid overflow problems.
	 */
	tmp->tm_wday = EPOCH_WDAY +
		((y - EPOCH_YEAR) % DAYSPERWEEK) *
		(DAYSPERNYEAR % DAYSPERWEEK) +
		leaps_thru_end_of(y - 1) -
		leaps_thru_end_of(EPOCH_YEAR - 1) +
		idays;
	tmp->tm_wday %= DAYSPERWEEK;
	if (tmp->tm_wday < 0)
		tmp->tm_wday += DAYSPERWEEK;
	tmp->tm_hour = (int) (rem / SECSPERHOUR);
	rem %= SECSPERHOUR;
	tmp->tm_min = (int) (rem / SECSPERMIN);

	/*
	 * A positive leap second requires a special representation. This uses
	 * "... ??:59:60" et seq.
	 */
	tmp->tm_sec = (int) (rem % SECSPERMIN) + hit;
	ip = mon_lengths[isleap(y)];
	for (tmp->tm_mon = 0; idays >= ip[tmp->tm_mon]; ++(tmp->tm_mon))
		idays -= ip[tmp->tm_mon];
	tmp->tm_mday = (int) (idays + 1);
	tmp->tm_isdst = 0;
	tmp->tm_gmtoff = offset;
	return tmp;

out_of_range:
	errno = EOVERFLOW;
	return nullptr;
}

static struct pg_tm *
gmtsub(pg_time_t const *timep, int32_t offset,
	   struct pg_tm *tmp)
{
	struct pg_tm *result;

	/* GMT timezone state data is kept here */
	static struct state *gmtptr = nullptr;

	if (gmtptr == nullptr)
	{
		/* Allocate on first use */
		gmtptr = (struct state *) palloc(sizeof(struct state));
		if (gmtptr == nullptr)
			return nullptr;		/* errno should be set by malloc */
		gmtload(gmtptr);
	}

	result = timesub(timep, offset, gmtptr, tmp);

	/*
	 * Could get fancy here and deliver something such as "+xx" or "-xx" if
	 * offset is non-zero, but this is no time for a treasure hunt.
	 */
	if (offset != 0)
		tmp->tm_zone = wildabbr;
	else
		tmp->tm_zone = gmtptr->chars;

	return result;
}

static struct pg_tm *
localsub(struct state const *sp, pg_time_t const *timep,
		 struct pg_tm *const tmp)
{
	const struct ttinfo *ttisp = nullptr;
	int			i = 0;
	struct pg_tm *result = nullptr;
	const pg_time_t t = *timep;

	if (sp == nullptr)
		return gmtsub(timep, 0, tmp);
	if ((sp->goback && t < sp->ats[0]) ||
		(sp->goahead && t > sp->ats[sp->timecnt - 1]))
	{
		pg_time_t	newt = t;
		pg_time_t	seconds = 0;
		pg_time_t	years = 0;

		if (t < sp->ats[0])
			seconds = sp->ats[0] - t;
		else
			seconds = t - sp->ats[sp->timecnt - 1];
		--seconds;
		years = (seconds / SECSPERREPEAT + 1) * YEARSPERREPEAT;
		seconds = years * AVGSECSPERYEAR;
		if (t < sp->ats[0])
			newt += seconds;
		else
			newt -= seconds;
		if (newt < sp->ats[0] ||
			newt > sp->ats[sp->timecnt - 1])
			return nullptr;		/* "cannot happen" */
		result = localsub(sp, &newt, tmp);
		if (result)
		{
			int64_t		newy;

			newy = result->tm_year;
			if (t < sp->ats[0])
				newy -= years;
			else
				newy += years;
			if (!(INT_MIN <= newy && newy <= INT_MAX))
				return nullptr;
			result->tm_year = newy;
		}
		return result;
	}
	if (sp->timecnt == 0 || t < sp->ats[0])
	{
		i = sp->defaulttype;
	}
	else
	{
		int			lo = 1;
		int			hi = sp->timecnt;

		while (lo < hi)
		{
			int			mid = (lo + hi) >> 1;

			if (t < sp->ats[mid])
				hi = mid;
			else
				lo = mid + 1;
		}
		i = (int) sp->types[lo - 1];
	}
	ttisp = &sp->ttis[i];

	/*
	 * To get (wrong) behavior that's compatible with System V Release 2.0
	 * you'd replace the statement below with t += ttisp->tt_utoff;
	 * timesub(&t, 0L, sp, tmp);
	 */
	result = timesub(&t, ttisp->tt_utoff, sp, tmp);
	if (result)
	{
		result->tm_isdst = ttisp->tt_isdst;
		result->tm_zone = unconstify(char *, &sp->chars[ttisp->tt_desigidx]);
	}
	return result;
}


struct pg_tm *
pg_localtime(const pg_time_t *timep, const pg_tz *tz)
{
    static thread_local struct pg_tm tm;
    return localsub(&tz->state, timep, &tm);
}

void
j2date(int jd, int *year, int *month, int *day)
{
	unsigned int julian = 0;
	unsigned int quad = 0;
	unsigned int extra = 0;
	int			y = 0;

	julian = jd;
	julian += 32044;
	quad = julian / 146097;
	extra = (julian - quad * 146097) * 4 + 3;
	julian += 60 + quad * 3 + extra / 146097;
	quad = julian / 1461;
	julian -= quad * 1461;
	y = julian * 4 / 1461;
	julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366))
		+ 123;
	y += quad * 4;
	*year = y - 4800;
	quad = julian * 2141 / 65536;
	*day = julian - 7834 * quad / 256;
	*month = (quad + 10) % MONTHS_PER_YEAR + 1;
}

void
dt2time(Timestamp jd, int *hour, int *min, int *sec, fsec_t *fsec)
{
	TimeOffset	time;

	time = jd;

	*hour = time / USECS_PER_HOUR;
	time -= (*hour) * USECS_PER_HOUR;
	*min = time / USECS_PER_MINUTE;
	time -= (*min) * USECS_PER_MINUTE;
	*sec = time / USECS_PER_SEC;
	*fsec = time - (*sec * USECS_PER_SEC);
}

int
timestamp2tm(Timestamp dt, int *tzp, struct pg_tm *tm, fsec_t *fsec, const char **tzn, pg_tz *attimezone)
{
	Timestamp	date;
	Timestamp	time;
	pg_time_t	utime;

	// XXX Check this
	// if (attimezone == nullptr)
	// 	attimezone = session_timezone;

	time = dt;
	TMODULO(time, date, USECS_PER_DAY);

	if (time < INT64CONST(0))
	{
		time += USECS_PER_DAY;
		date -= 1;
	}

	/* add offset to go from J2000 back to standard Julian date */
	date += POSTGRES_EPOCH_JDATE;

	/* Julian day routine does not work for negative Julian days */
	if (date < 0 || date > (Timestamp) INT_MAX)
		return -1;

	j2date((int) date, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
	dt2time(time, &tm->tm_hour, &tm->tm_min, &tm->tm_sec, fsec);

	/* Done if no TZ conversion wanted */
	if (tzp == nullptr)
	{
		tm->tm_isdst = -1;
		tm->tm_gmtoff = 0;
		tm->tm_zone = nullptr;
		if (tzn != nullptr)
			*tzn = nullptr;
		return 0;
	}

	/*
	 * If the time falls within the range of pg_time_t, use pg_localtime() to
	 * rotate to the local time zone.
	 *
	 * First, convert to an integral timestamp, avoiding possibly
	 * platform-specific roundoff-in-wrong-direction errors, and adjust to
	 * Unix epoch.  Then see if we can convert to pg_time_t without loss. This
	 * coding avoids hardwiring any assumptions about the width of pg_time_t,
	 * so it should behave sanely on machines without int64.
	 */
	dt = (dt - *fsec) / USECS_PER_SEC +
		(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY;
	utime = (pg_time_t) dt;
	if ((Timestamp) utime == dt)
	{
		struct pg_tm *tx = pg_localtime(&utime, attimezone);

		tm->tm_year = tx->tm_year + 1900;
		tm->tm_mon = tx->tm_mon + 1;
		tm->tm_mday = tx->tm_mday;
		tm->tm_hour = tx->tm_hour;
		tm->tm_min = tx->tm_min;
		tm->tm_sec = tx->tm_sec;
		tm->tm_isdst = tx->tm_isdst;
		tm->tm_gmtoff = tx->tm_gmtoff;
		tm->tm_zone = tx->tm_zone;
		*tzp = -tm->tm_gmtoff;
		if (tzn != nullptr)
			*tzn = tm->tm_zone.c_str();
	}
	else
	{
		/*
		 * When out of range of pg_time_t, treat as GMT
		 */
		*tzp = 0;
		/* Mark this as *no* time zone available */
		tm->tm_isdst = -1;
		tm->tm_gmtoff = 0;
		tm->tm_zone = nullptr;
		if (tzn != nullptr)
			*tzn = nullptr;
	}

	return 0;
}

static char *
AppendSeconds(char *cp, int sec, fsec_t fsec, int precision, bool fillzeros)
{
	assert(precision >= 0);

	if (fillzeros)
		cp = pg_ultostr_zeropad(cp, abs(sec), 2);
	else
		cp = pg_ultostr(cp, abs(sec));

	/* fsec_t is just an int32 */
	if (fsec != 0)
	{
		int32_t		value = abs(fsec);
		char	   *end = &cp[precision + 1];
		bool		gotnonzero = false;

		*cp++ = '.';

		/*
		 * Append the fractional seconds part.  Note that we don't want any
		 * trailing zeros here, so since we're building the number in reverse
		 * we'll skip appending zeros until we've output a non-zero digit.
		 */
		while (precision--)
		{
			int32_t		oldval = value;
			int32_t		remainder;

			value /= 10;
			remainder = oldval - value * 10;

			/* check if we got a non-zero */
			if (remainder)
				gotnonzero = true;

			if (gotnonzero)
				cp[precision] = '0' + remainder;
			else
				end = &cp[precision];
		}

		/*
		 * If we still have a non-zero value then precision must have not been
		 * enough to print the number.  We punt the problem to pg_ultostr(),
		 * which will generate a correct answer in the minimum valid width.
		 */
		if (value)
			return pg_ultostr(cp, abs(fsec));

		return end;
	}
	else
		return cp;
}

void
EncodeSpecialTimestamp(Timestamp dt, char *str)
{
	if (str == nullptr)
    {
        LOG_ERROR("invalid buffer passed to EncodeSpecialTimestamp");
        return;
    }

    std::string_view text;
    if (TIMESTAMP_IS_NOBEGIN(dt))
        text = EARLY;
    else if (TIMESTAMP_IS_NOEND(dt))
        text = LATE;
    else
    {
        LOG_ERROR("invalid argument for EncodeSpecialTimestamp");
        return;
    }

    // Write into str, ensuring null termination
    auto result = std::format_to_n(str, 63, "{}", text);
    str[result.size] = '\0';
}

void EncodeSpecialDate(DateADT dt, char* str)
{
    if (str == nullptr)
    {
        LOG_ERROR("invalid buffer passed to EncodeSpecialDate");
        return;
    }

    std::string_view text;

    if (DATE_IS_NOBEGIN(dt))
        text = EARLY;
    else if (DATE_IS_NOEND(dt))
        text = LATE;
    else
    {
        LOG_ERROR("invalid argument for EncodeSpecialDate");
        return;
    }

    // Write into str safely, ensuring null termination
    auto result = std::format_to_n(str, 63, "{}", text);
    str[result.size] = '\0';
}
static char *
EncodeTimezone(char *str, int tz, int style)
{
	int			hour = 0;
	int			min = 0;
	int			sec = 0;

	sec = abs(tz);
	min = sec / SECS_PER_MINUTE;
	sec -= min * SECS_PER_MINUTE;
	hour = min / MINS_PER_HOUR;
	min -= hour * MINS_PER_HOUR;

	/* TZ is negated compared to sign we wish to display ... */
	*str++ = (tz <= 0 ? '+' : '-');

	if (sec != 0)
	{
		str = pg_ultostr_zeropad(str, hour, 2);
		*str++ = ':';
		str = pg_ultostr_zeropad(str, min, 2);
		*str++ = ':';
		str = pg_ultostr_zeropad(str, sec, 2);
	}
	else if (min != 0 || style == USE_XSD_DATES)
	{
		str = pg_ultostr_zeropad(str, hour, 2);
		*str++ = ':';
		str = pg_ultostr_zeropad(str, min, 2);
	}
	else
		str = pg_ultostr_zeropad(str, hour, 2);
	return str;
}

int
time2tm(TimeADT time, struct pg_tm *tm, fsec_t *fsec)
{
	tm->tm_hour = time / USECS_PER_HOUR;
	time -= tm->tm_hour * USECS_PER_HOUR;
	tm->tm_min = time / USECS_PER_MINUTE;
	time -= tm->tm_min * USECS_PER_MINUTE;
	tm->tm_sec = time / USECS_PER_SEC;
	time -= tm->tm_sec * USECS_PER_SEC;
	*fsec = time;
	return 0;
}

void
EncodeTimeOnly(struct pg_tm *tm, fsec_t fsec, bool print_tz, int tz, int style, char *str)
{
	str = pg_ultostr_zeropad(str, tm->tm_hour, 2);
	*str++ = ':';
	str = pg_ultostr_zeropad(str, tm->tm_min, 2);
	*str++ = ':';
	str = AppendSeconds(str, tm->tm_sec, fsec, MAX_TIME_PRECISION, true);
	if (print_tz)
		str = EncodeTimezone(str, tz, style);
	*str = '\0';
}

int
timetz2tm(TimeTzADT *time, struct pg_tm *tm, fsec_t *fsec, int *tzp)
{
	TimeOffset	trem = time->time;

	tm->tm_hour = trem / USECS_PER_HOUR;
	trem -= tm->tm_hour * USECS_PER_HOUR;
	tm->tm_min = trem / USECS_PER_MINUTE;
	trem -= tm->tm_min * USECS_PER_MINUTE;
	tm->tm_sec = trem / USECS_PER_SEC;
	*fsec = trem - tm->tm_sec * USECS_PER_SEC;

	if (tzp != nullptr)
		*tzp = time->zone;

	return 0;
}

/*
 * Variant of above that's specialized to timestamp case.
 *
 * Returns a pointer to the new end of string.  No NUL terminator is put
 * there; callers are responsible for NUL terminating str themselves.
 */
static char *
AppendTimestampSeconds(char *cp, struct pg_tm *tm, fsec_t fsec)
{
	return AppendSeconds(cp, tm->tm_sec, fsec, MAX_TIMESTAMP_PRECISION, true);
}

int
date2j(int year, int month, int day)
{
	int			julian = 0;
	int			century = 0;

	if (month > 2)
	{
		month += 1;
		year += 4800;
	}
	else
	{
		month += 13;
		year += 4799;
	}

	century = year / 100;
	julian = year * 365 - 32167;
	julian += year / 4 - century + century / 4;
	julian += 7834 * month / 256 + day;

	return julian;
}

int
j2day(int date)
{
	date += 1;
	date %= 7;
	/* Cope if division truncates towards zero, as it probably does */
	if (date < 0)
		date += 7;

	return date;
}

void EncodeDateTime(struct pg_tm *tm,
                    fsec_t fsec,
                    bool print_tz,
                    int tz,
                    const char *tzn,
                    int style,
                    char *str)
{
    int day = 0;
    assert(tm->tm_mon >= 1 && tm->tm_mon <= MONTHS_PER_YEAR);

    if (tm->tm_isdst < 0)
        print_tz = false;

    size_t remaining = 64;     // remaining buffer size

    auto write_str = [&](std::string_view s) {
        if (remaining == 0) return; // nothing left
        auto result = std::format_to_n(str, remaining - 1, "{}", s); // reserve 1 for '\0'
        size_t written = result.size;
        if (written >= remaining)
            written = remaining - 1; // truncate if necessary
        str += written;
        remaining -= written;
        *str = '\0'; // maintain null termination
    };

    switch (style)
    {
        case USE_ISO_DATES:
        case USE_XSD_DATES:
            str = pg_ultostr_zeropad(str, (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1), 4);
            if (remaining > 1) { *str++ = (style == USE_ISO_DATES ? ' ' : 'T'); --remaining; }
            str = pg_ultostr_zeropad(str, tm->tm_mon, 2);
            if (remaining > 1) { *str++ = '-'; --remaining; }
            str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
            if (remaining > 1) { *str++ = ' '; --remaining; }
            str = pg_ultostr_zeropad(str, tm->tm_hour, 2);
            if (remaining > 1) { *str++ = ':'; --remaining; }
            str = pg_ultostr_zeropad(str, tm->tm_min, 2);
            if (remaining > 1) { *str++ = ':'; --remaining; }
            str = AppendTimestampSeconds(str, tm, fsec);
            if (print_tz)
                str = EncodeTimezone(str, tz, style);
            break;

        case USE_SQL_DATES:
            if (DateOrder == DATEORDER_DMY)
            {
                str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
                if (remaining > 1) { *str++ = '/'; --remaining; }
                str = pg_ultostr_zeropad(str, tm->tm_mon, 2);
            }
            else
            {
                str = pg_ultostr_zeropad(str, tm->tm_mon, 2);
                if (remaining > 1) { *str++ = '/'; --remaining; }
                str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
            }
            if (remaining > 1) { *str++ = '/'; --remaining; }
            str = pg_ultostr_zeropad(str, (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1), 4);
            if (remaining > 1) { *str++ = ' '; --remaining; }
            str = pg_ultostr_zeropad(str, tm->tm_hour, 2);
            if (remaining > 1) { *str++ = ':'; --remaining; }
            str = pg_ultostr_zeropad(str, tm->tm_min, 2);
            if (remaining > 1) { *str++ = ':'; --remaining; }
            str = AppendTimestampSeconds(str, tm, fsec);

            if (print_tz)
            {
                if (tzn)
                {
                    size_t tznlen = strnlen(tzn, MAXTZLEN);
                    write_str(" ");
                    write_str(std::string_view(tzn, tznlen));
                }
                else
                    str = EncodeTimezone(str, tz, style);
            }
            break;

        case USE_GERMAN_DATES:
            str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
            if (remaining > 1) { *str++ = '.'; --remaining; }
            str = pg_ultostr_zeropad(str, tm->tm_mon, 2);
            if (remaining > 1) { *str++ = '.'; --remaining; }
            str = pg_ultostr_zeropad(str, (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1), 4);
            if (remaining > 1) { *str++ = ' '; --remaining; }
            str = pg_ultostr_zeropad(str, tm->tm_hour, 2);
            if (remaining > 1) { *str++ = ':'; --remaining; }
            str = pg_ultostr_zeropad(str, tm->tm_min, 2);
            if (remaining > 1) { *str++ = ':'; --remaining; }
            str = AppendTimestampSeconds(str, tm, fsec);

            if (print_tz)
            {
                if (tzn){
					size_t tznlen = strnlen(tzn, MAXTZLEN);
                    write_str(" ");
                    write_str(std::string_view(tzn, tznlen));
				}
                else
                    str = EncodeTimezone(str, tz, style);
            }
            break;

        case USE_POSTGRES_DATES:
        default:
            day = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday);
            tm->tm_wday = j2day(day);
            if (remaining >= 3) { memcpy(str, days[tm->tm_wday], 3); str += 3; remaining -= 3; }
            if (remaining > 1) { *str++ = ' '; --remaining; }
            if (DateOrder == DATEORDER_DMY)
            {
                str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
                if (remaining > 1) { *str++ = ' '; --remaining; }
                if (remaining >= 3) { memcpy(str, months[tm->tm_mon - 1], 3); str += 3; remaining -= 3; }
            }
            else
            {
                if (remaining >= 3) { memcpy(str, months[tm->tm_mon - 1], 3); str += 3; remaining -= 3; }
                if (remaining > 1) { *str++ = ' '; --remaining; }
                str = pg_ultostr_zeropad(str, tm->tm_mday, 2);
            }
            if (remaining > 1) { *str++ = ' '; --remaining; }
            str = pg_ultostr_zeropad(str, tm->tm_hour, 2);
            if (remaining > 1) { *str++ = ':'; --remaining; }
            str = pg_ultostr_zeropad(str, tm->tm_min, 2);
            if (remaining > 1) { *str++ = ':'; --remaining; }
            str = AppendTimestampSeconds(str, tm, fsec);
            if (remaining > 1) { *str++ = ' '; --remaining; }
            str = pg_ultostr_zeropad(str, (tm->tm_year > 0) ? tm->tm_year : -(tm->tm_year - 1), 4);

            if (print_tz)
            {
                if (tzn)
                {
                    size_t tznlen = strnlen(tzn, MAXTZLEN);
                    write_str(" ");
                    write_str(std::string_view(tzn, tznlen));
                }
                else
                {
                    if (remaining > 1) { *str++ = ' '; --remaining; }
                    str = EncodeTimezone(str, tz, style);
                }
            }
            break;
    }

    if (tm->tm_year <= 0)
    {
        if (remaining >= 3) { memcpy(str, " BC", 3); str += 3; remaining -= 3; }
    }

    *str = '\0'; // always null-terminate
}
