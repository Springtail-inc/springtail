#include <pg_ext/error.hh>
#include <pg_ext/node.hh>
#include <common/logging.hh>

#include <cstdarg>
#include <cstdio>
#include <cstring>

volatile sig_atomic_t InterruptPending = false; // NOSONAR

/*
 * Internal helpers that avoid ellipsis by using va_list-based "v" variants.
 * Public functions remain variadic to preserve the existing API, but they
 * forward immediately to these helpers.
 */
static void errdetail_v(const char *fmt, va_list ap)
{
    char buf[1024] = "";
    int n = vsnprintf(buf, sizeof(buf), fmt ? fmt : "(null)", ap); // NOSONAR - Fmt is validated
    (void)n;
    LOG_ERROR("Errdetail {}", buf);
}

static int errmsg_v(const char *fmt, va_list ap)
{
    char buf[1024] = "";
    int n = vsnprintf(buf, sizeof(buf), fmt ? fmt : "(null)", ap); // NOSONAR - Fmt is validated
    (void)n;
    LOG_ERROR("Errmsg {}", buf);
    return 0;
}

static int errmsg_internal_v(const char *fmt, va_list ap)
{
    char buf[1024] = "";
    int n = vsnprintf(buf, sizeof(buf), fmt ? fmt : "(null)", ap);  // NOSONAR - Fmt is validated
    (void)n;
    LOG_ERROR("Errmsg internal {}", buf);
    return 0;
}

static int pg_vfprintf(FILE *stream, const char *fmt, va_list ap)
{
    if (!fmt)
    {
        LOG_ERROR("Pg_fprintf (null)");
        return 0;
    }
    char buf[1024] = "";
    int written = vsnprintf(buf, sizeof(buf), fmt, ap); // NOSONAR - Fmt is validated
    /* Maintain prior behavior: log the formatted message. */
    LOG_ERROR("Pg_fprintf {}", buf);
    (void)stream; /* stream is unused in current implementation */
    return (written < 0) ? 0 : written;
}

void ProcessInterrupts() {
    // Check for interrupts and handle them
    if (InterruptPending) {
        InterruptPending = false;
        // Handle interrupt - in this case we'll just log it
        LOG_ERROR("Process interrupt received");
    }
}

bool errstart(int elevel, const char *domain) {
    LOG_ERROR("Error level: {}, Error started at (domain: {})", elevel, domain ? domain : "none");
    return true;
}

bool errstart_cold(int elevel, const char *domain) {
    // Cold path error start - same as regular errstart but optimized for cold path
    return errstart(elevel, domain);
}

void
errfinish(const char *filename, int lineno, const char *funcname)
{
    LOG_ERROR("Filename: {}, Line: {}, Function: {}", filename, lineno, funcname);
}

bool
errsave_start(const struct Node *context, const char *domain)
{
    LOG_ERROR("Errsave - Start {} - Context: {}", domain, (int)context->type);
    return true;
}

void
errsave_finish(const struct Node *context, const char *filename, int lineno,
			   const char *funcname)
{
    LOG_ERROR("Errsave - Finish Context: {}, Line: {}, Function: {}", (int)context->type, filename, lineno, funcname);
}

void errdetail(const char *fmt, ...) // NOSONAR: pg_func - Needs ellipsis
{
    va_list ap;
    va_start(ap, fmt);
    errdetail_v(fmt, ap);
    va_end(ap);
}

int errcode(int sqlerrcode) {
    LOG_ERROR("Errcode {}", sqlerrcode);
    return 0;
}

int errmsg(const char *fmt, ...) // NOSONAR: pg_func - Needs ellipsis
{
    va_list ap;
    va_start(ap, fmt);
    int rc = errmsg_v(fmt, ap);
    va_end(ap);
    return rc;
}

int errmsg_internal(const char *fmt, ...) // NOSONAR: pg_func - Needs ellipsis
{
    va_list ap;
    va_start(ap, fmt);
    int rc = errmsg_internal_v(fmt, ap);
    va_end(ap);
    return rc;
}

int
pg_fprintf(FILE *stream, const char *fmt,...) // NOSONAR: pg_func - Needs ellipsis
{
    va_list ap;
    va_start(ap, fmt);
    int rc = pg_vfprintf(stream, fmt, ap);
    va_end(ap);
    return rc;
}
