#include <pg_ext/error.hh>
#include <common/logging.hh>

#include <cstdarg>
#include <cstdio>
#include <cstring>

// Global variables for error state
static thread_local int current_elevel = 0;
static thread_local int current_sqlcode = 0;
static thread_local char current_error_message[1024] = {0};
static thread_local bool error_in_progress = false;

volatile sig_atomic_t InterruptPending = false;

void ProcessInterrupts() {
    // Check for interrupts and handle them
    if (InterruptPending) {
        InterruptPending = false;
        // Handle interrupt - in this case we'll just log it
        fprintf(stderr, "Process interrupt received\n");
    }
}

bool errstart(int elevel, const char *domain) {
    // Reset error state
    current_elevel = elevel;
    current_sqlcode = 0;
    current_error_message[0] = '\0';
    error_in_progress = true;

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
    LOG_ERROR("Filename: %s, Line: %d, Function: %s, Error: %s", filename, lineno, funcname, current_error_message);
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

void errdetail(const char *fmt, ...) {
    LOG_ERROR("Errdetail %s", fmt);
}

int errcode(int sqlerrcode) {
    LOG_ERROR("Errcode %d", sqlerrcode);
    return 0;
}

int errmsg(const char *fmt, ...) {
    LOG_ERROR("Errmsg %s", fmt);

    return 0;
}

int errmsg_internal(const char *fmt, ...) {
    LOG_ERROR("Errmsg internal %s", fmt);

    return 0;
}

int
pg_fprintf(FILE *stream, const char *fmt,...)
{
    LOG_ERROR("Pg_fprintf %s", fmt);
	return 0;
}


bool GetDefaultCharSignedness(void) {
    LOG_ERROR("GetDefaultCharSignedness");
    return true;
}
