#pragma once

#include <cstdint>

#include <pg_ext/export.hh>
#include <pg_ext/fmgr.hh>
#include <pg_ext/numeric.hh>

#include <common/logging.hh>

using DateADT = int32_t;
using TimeADT = int64_t;
using fsec_t = int32_t;
using Timestamp = int64_t;
using TimestampTz = int64_t;
using TimeOffset = int64_t;
using pg_time_t = int64_t;

constexpr int MAXDATELEN = 128;
constexpr int MAXTZLEN = 10;

constexpr int64_t USECS_PER_HOUR = INT64CONST(3600000000);
constexpr int64_t USECS_PER_MINUTE = INT64CONST(60000000);
constexpr int64_t USECS_PER_SEC = INT64CONST(1000000);
constexpr int64_t USECS_PER_MSEC = INT64CONST(1000);
constexpr int64_t USECS_PER_USEC = INT64CONST(1);
constexpr int64_t USECS_PER_DAY = INT64CONST(86400000000);

constexpr int YEARSPERREPEAT = 400; /* years before a Gregorian repeat */
constexpr int64_t AVGSECSPERYEAR = INT64CONST(31556952);
constexpr int64_t SECSPERREPEAT = YEARSPERREPEAT * AVGSECSPERYEAR;
constexpr int SECSPERREPEAT_BITS = 34;

constexpr int SECSPERMIN = 60;
constexpr int MINSPERHOUR = 60;
constexpr int HOURSPERDAY = 24;
constexpr int DAYSPERWEEK = 7;
constexpr int DAYSPERNYEAR = 365;
constexpr int DAYSPERLYEAR = 366;
constexpr int SECSPERHOUR = SECSPERMIN * MINSPERHOUR;
constexpr int SECSPERDAY = SECSPERHOUR * HOURSPERDAY;
constexpr int MONSPERYEAR = 12;

constexpr int UNIX_EPOCH_JDATE = 2440588; /* == date2j(1970, 1, 1) */

constexpr int PG_INT32_MIN = (-0x7FFFFFFF-1);
constexpr int PG_INT32_MAX = 0x7FFFFFFF;

constexpr int SECS_PER_YEAR = 36525 * 864;	/* avoid floating-point computation */
constexpr int SECS_PER_DAY = 86400;
constexpr int SECS_PER_HOUR = 3600;
constexpr int SECS_PER_MINUTE = 60;
constexpr int MINS_PER_HOUR = 60;

/* valid DateStyle values */
constexpr int USE_POSTGRES_DATES = 0;
constexpr int USE_ISO_DATES = 1;
constexpr int USE_SQL_DATES = 2;
constexpr int USE_GERMAN_DATES = 3;
constexpr int USE_XSD_DATES = 4;

/* valid DateOrder values */
constexpr int DATEORDER_YMD = 0;
constexpr int DATEORDER_DMY = 1;
constexpr int DATEORDER_MDY = 2;

constexpr int TM_SUNDAY = 0;
constexpr int TM_MONDAY = 1;
constexpr int TM_TUESDAY = 2;
constexpr int TM_WEDNESDAY = 3;
constexpr int TM_THURSDAY = 4;
constexpr int TM_FRIDAY = 5;
constexpr int TM_SATURDAY = 6;

constexpr int EPOCH_YEAR = 1970;
constexpr int EPOCH_WDAY = TM_THURSDAY;

constexpr int TM_YEAR_BASE = 1900;

constexpr int MAX_TIMESTAMP_PRECISION = 6;
constexpr int MAX_TIME_PRECISION = 6;

constexpr int64_t TIMESTAMP_MINUS_INFINITY = INT64_MIN;
constexpr int64_t TIMESTAMP_INFINITY = INT64_MAX;

constexpr int64_t DT_NOBEGIN = TIMESTAMP_MINUS_INFINITY;
constexpr int64_t DT_NOEND = TIMESTAMP_INFINITY;

constexpr const char *EARLY = "-infinity";
constexpr const char *LATE = "infinity";

constexpr const char *TZDEFRULESTRING = ",M3.2.0,M11.1.0";

constexpr int DATEVAL_NOBEGIN = PG_INT32_MIN;
constexpr int DATEVAL_NOEND = PG_INT32_MAX;

constexpr bool DATE_IS_NOEND(int j) { return j == DATEVAL_NOEND; }
constexpr void DATE_NOBEGIN(int j) { j = DATEVAL_NOBEGIN; }
constexpr bool DATE_IS_NOBEGIN(int j) { return j == DATEVAL_NOBEGIN; }
constexpr bool DATE_NOT_FINITE(int j) { return DATE_IS_NOBEGIN(j) || DATE_IS_NOEND(j); }

constexpr int POSTGRES_EPOCH_JDATE = 2451545; /* == date2j(2000, 1, 1) */

constexpr void TIMESTAMP_NOBEGIN(int64_t j) { j = DT_NOBEGIN; }
constexpr bool TIMESTAMP_IS_NOBEGIN(int64_t j) { return j == DT_NOBEGIN; }

constexpr void TIMESTAMP_NOEND(int64_t j) { j = DT_NOEND; }

constexpr bool TIMESTAMP_IS_NOEND(int64_t j) { return j == DT_NOEND; }

constexpr bool TIMESTAMP_NOT_FINITE(int64_t j) { return TIMESTAMP_IS_NOBEGIN(j) || TIMESTAMP_IS_NOEND(j); }

constexpr double DAYS_PER_YEAR = 365.25; /* assumes leap year every four years */
constexpr int MONTHS_PER_YEAR = 12;

#define TMODULO(t,q,u) \
do { \
	(q) = ((t) / (u)); \
	if ((q) != 0) (t) -= ((q) * (u)); \
} while(0)

static const int year_lengths[2] = {
	DAYSPERNYEAR, DAYSPERLYEAR
};

static const int mon_lengths[2][MONSPERYEAR] = {
	{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
	{31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
};

constexpr int TZ_MAX_TIMES = 2000;
/* This must be at least 17 for Europe/Samara and Europe/Vilnius.  */
constexpr int TZ_MAX_TYPES = 256;		/* Limited by what (unsigned char)'s can hold */
constexpr int TZ_MAX_CHARS = 50;		/* Maximum number of abbreviation characters */
 /* (limited by what unsigned chars can hold) */
constexpr int TZ_MAX_LEAPS = 50;		/* Maximum number of leap second corrections */
constexpr int TZ_STRLEN_MAX = 255;

/*
 * Finally, some convenience items.
 */
#define TYPE_BIT(type)	(sizeof (type) * CHAR_BIT)
#define TYPE_SIGNED(type) (((type) -1) < 0)
#define TWOS_COMPLEMENT(t) ((t) ~ (t) 0 < 0)

/*
* Max and min values of the integer type T, of which only the bottom
* B bits are used, and where the highest-order used bit is considered
* to be a sign bit if T is signed.
*/
#define MAXVAL(t, b)						\
((t) (((t) 1 << ((b) - 1 - TYPE_SIGNED(t)))			\
    - 1 + ((t) 1 << ((b) - 1 - TYPE_SIGNED(t)))))
#define MINVAL(t, b)						\
((t) (TYPE_SIGNED(t) ? - TWOS_COMPLEMENT(t) - MAXVAL(t, b) : 0))

/* The extreme time values, assuming no padding.  */
#define TIME_T_MIN MINVAL(pg_time_t, TYPE_BIT(pg_time_t))
#define TIME_T_MAX MAXVAL(pg_time_t, TYPE_BIT(pg_time_t))

constexpr char WILDABBR[] = "   ";
static const std::string wildabbr = WILDABBR;

static const char gmt[] = "GMT";

#define TYPE_SIGNED(type) (((type) -1) < 0)

#define isleap(y) (((y) % 4) == 0 && (((y) % 100) != 0 || ((y) % 400) == 0))

#define SMALLEST(a, b)	(((a) < (b)) ? (a) : (b))
#define BIGGEST(a, b)	(((a) > (b)) ? (a) : (b))

#define TWOS_COMPLEMENT(t) ((t) ~ (t) 0 < 0)

constexpr const char *TZDEFAULT = "/etc/localtime";
struct TimeTzADT
{
	TimeADT		time;			/* all time units other than months and years */
	int32_t		zone;			/* numeric time zone, in seconds */
};

struct pg_tm
{
	int			tm_sec;
	int			tm_min;
	int			tm_hour;
	int			tm_mday;
	int			tm_mon;			/* see above */
	int			tm_year;		/* see above */
	int			tm_wday;
	int			tm_yday;
	int			tm_isdst;
	long int	tm_gmtoff;
	std::string tm_zone;
};

struct ttinfo
{								/* time type information */
	int32_t		tt_utoff;		/* UT offset in seconds */
	bool		tt_isdst;		/* used to set tm_isdst */
	int			tt_desigidx;	/* abbreviation list index */
	bool		tt_ttisstd;		/* transition is std time */
	bool		tt_ttisut;		/* transition is UT */
};

struct lsinfo
{								/* leap second information */
	pg_time_t	ls_trans;		/* transition time */
	int64_t		ls_corr;		/* correction to apply */
};

struct state
{
	int			leapcnt;
	int			timecnt;
	int			typecnt;
	int			charcnt;
	bool		goback;
	bool		goahead;
	pg_time_t	ats[TZ_MAX_TIMES];
	unsigned char types[TZ_MAX_TIMES];
	struct ttinfo ttis[TZ_MAX_TYPES];
	char		chars[BIGGEST(BIGGEST(TZ_MAX_CHARS + 1, 4 /* sizeof gmt */ ),
							  (2 * (TZ_STRLEN_MAX + 1)))];
	struct lsinfo lsis[TZ_MAX_LEAPS];

	/*
	 * The time type to use for early times or if no transitions. It is always
	 * zero for recent tzdb releases. It might be nonzero for data from tzdb
	 * 2018e or earlier.
	 */
	int			defaulttype;
};

struct pg_tz
{
	/* TZname contains the canonically-cased name of the timezone */
	char		TZname[TZ_STRLEN_MAX + 1];
	struct state state;
};

struct tzhead
{
	char		tzh_magic[4];	/* TZ_MAGIC */
	char		tzh_version[1]; /* '\0' or '2' or '3' as of 2013 */
	char		tzh_reserved[15];	/* reserved; must be zero */
	char		tzh_ttisutcnt[4];	/* coded number of trans. time flags */
	char		tzh_ttisstdcnt[4];	/* coded number of trans. time flags */
	char		tzh_leapcnt[4]; /* coded number of leap seconds */
	char		tzh_timecnt[4]; /* coded number of transition times */
	char		tzh_typecnt[4]; /* coded number of local time types */
	char		tzh_charcnt[4]; /* coded number of abbr. chars */
};

enum class r_type
{
	JULIAN_DAY,					/* Jn = Julian day */
	DAY_OF_YEAR,				/* n = day of year */
	MONTH_NTH_DAY_OF_WEEK		/* Mm.n.d = month, week, day of week */
};

struct rule
{
	enum r_type r_type;			/* type of rule */
	int			r_day;			/* day number of rule */
	int			r_week;			/* week number of rule */
	int			r_mon;			/* month number of rule */
	int32_t		r_time;			/* transition time of rule */
};

/* Input buffer for data read from a compiled tz file.  */
union input_buffer
{
	/* The first part of the buffer, interpreted as a header.  */
	struct tzhead tzhead;

	/* The entire buffer.  */
	char		buf[2 * sizeof(struct tzhead) + 2 * sizeof(struct state)
					+ 4 * TZ_MAX_TIMES];
};

union local_storage
{
	/* The results of analyzing the file's contents after it is opened.  */
	struct file_analysis
	{
		/* The input buffer.  */
		union input_buffer u;

		/* A temporary state used for parsing a TZ string in the file.  */
		struct state st;
	}			u;

	/* We don't need the "fullname" member */
};

/* Global variables */
extern const char * const months[];
extern const char * const days[];
extern const int DateOrder;

extern "C" PGEXT_API DateADT DatumGetDateADT(Datum X);
extern "C" PGEXT_API TimeADT DatumGetTimeADT(Datum X);
extern "C" PGEXT_API TimeTzADT *DatumGetTimeTzADTP(Datum X);
extern "C" PGEXT_API Timestamp DatumGetTimestamp(Datum X);
extern "C" PGEXT_API TimestampTz DatumGetTimestampTz(Datum X);
extern "C" PGEXT_API TimeOffset DatumGetTimeOffset(Datum X);

extern "C" PGEXT_API int timestamp2tm(Timestamp dt, int *tzp, struct pg_tm *tm, fsec_t *fsec, const char **tzn, pg_tz *attimezone);
extern "C" PGEXT_API void EncodeDateOnly(struct pg_tm *tm, int style, char *str);
extern "C" PGEXT_API void EncodeDateTime(struct pg_tm *tm, fsec_t fsec, bool print_tz, int tz, const char *tzn, int style, char *str);
extern "C" PGEXT_API void EncodeSpecialTimestamp(Timestamp dt, char *str);
extern "C" PGEXT_API void EncodeSpecialDate(DateADT dt, char *str);
extern "C" PGEXT_API void EncodeTimeOnly(struct pg_tm *tm, fsec_t fsec, bool print_tz, int tz, int style, char *str);
extern "C" PGEXT_API int timetz2tm(TimeTzADT *time, struct pg_tm *tm, fsec_t *fsec, int *tzp);
extern "C" PGEXT_API int time2tm(TimeADT time, struct pg_tm *tm, fsec_t *fsec);
extern "C" PGEXT_API void j2date(int jd, int *year, int *month, int *day);
