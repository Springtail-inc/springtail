#include <pg_ext/error.hh>
#include <common/logging.hh>

#include <cstdarg>
#include <cstdio>
#include <cstring>

volatile sig_atomic_t InterruptPending = false;

/*
 * Internal helpers that avoid ellipsis by using va_list-based "v" variants.
 * Public functions remain variadic to preserve the existing API, but they
 * forward immediately to these helpers.
 */
static void errdetail_v(const char *fmt, va_list ap)
{
    char buf[1024];
    int n = vsnprintf(buf, sizeof(buf), fmt ? fmt : "(null)", ap);
    (void)n;
    LOG_ERROR("Errdetail %s", buf);
}

static int errmsg_v(const char *fmt, va_list ap)
{
    char buf[1024];
    int n = vsnprintf(buf, sizeof(buf), fmt ? fmt : "(null)", ap);
    (void)n;
    LOG_ERROR("Errmsg %s", buf);
    return 0;
}

static int errmsg_internal_v(const char *fmt, va_list ap)
{
    char buf[1024];
    int n = vsnprintf(buf, sizeof(buf), fmt ? fmt : "(null)", ap);
    (void)n;
    LOG_ERROR("Errmsg internal %s", buf);
    return 0;
}

static int pg_vfprintf(FILE *stream, const char *fmt, va_list ap)
{
    if (!fmt)
    {
        LOG_ERROR("Pg_fprintf (null)");
        return 0;
    }
    char buf[1024];
    int written = vsnprintf(buf, sizeof(buf), fmt, ap);
    /* Maintain prior behavior: log the formatted message. */
    LOG_ERROR("Pg_fprintf %s", buf);
    (void)stream; /* stream is unused in current implementation */
    return (written < 0) ? 0 : written;
}

void ProcessInterrupts() {
    // Check for interrupts and handle them
    if (InterruptPending) {
        InterruptPending = false;
        // Handle interrupt - in this case we'll just log it
        fprintf(stderr, "Process interrupt received\n");
    }
}

bool errstart(int elevel, const char *domain) {
    (void)elevel; // silence unused parameter warning
    fprintf(stderr, "\nError started at (domain: %s)", domain ? domain : "none");
    return true;
}

bool errstart_cold(int elevel, const char *domain) {
    // Cold path error start - same as regular errstart but optimized for cold path
    return errstart(elevel, domain);
}

void
errfinish(const char *filename, int lineno, const char *funcname)
{
    LOG_ERROR("Filename: %s, Line: %d, Function: %s", filename, lineno, funcname);
}

bool
errsave_start(struct Node *context, const char *domain)
{
    LOG_ERROR("Errsave - Start %s", domain);
    return true;
}

void
errsave_finish(struct Node *context, const char *filename, int lineno,
			   const char *funcname)
{
    LOG_ERROR("Errsave - Finish %s, Line: %d, Function: %s", filename, lineno, funcname);
}

void errdetail(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    errdetail_v(fmt, ap);
    va_end(ap);
}

int errcode(int sqlerrcode) {
    LOG_ERROR("Errcode %d", sqlerrcode);
    return 0;
}

int errmsg(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int rc = errmsg_v(fmt, ap);
    va_end(ap);
    return rc;
}

int errmsg_internal(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int rc = errmsg_internal_v(fmt, ap);
    va_end(ap);
    return rc;
}

int
pg_fprintf(FILE *stream, const char *fmt,...)
{
    va_list ap;
    va_start(ap, fmt);
    int rc = pg_vfprintf(stream, fmt, ap);
    va_end(ap);
    return rc;
}
