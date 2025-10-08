#include <pg_ext/error.hh>

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

void errfinish(int dummy, ...) {
    // XXX Stubbed for now
}

void errsave_start(int dummy, ...) {
    // XXX Stubbed for now
    if (!error_in_progress) {
        return;
    }
}

void errsave_finish(int dummy, ...) {
    // XXX Stubbed for now
    if (!error_in_progress) {
        return;
    }
}

void errdetail(const char *fmt, ...) {
    if (!error_in_progress) {
        return;
    }

    va_list args;
    va_start(args, fmt);
    vsnprintf(current_error_message, sizeof(current_error_message), fmt, args);
    va_end(args);
}

int errcode(int sqlerrcode) {
    if (!error_in_progress) {
        return 0;
    }
    current_sqlcode = sqlerrcode;
    return 0;
}

int errmsg(const char *fmt, ...) {
    if (!error_in_progress) {
        return 0;
    }

    va_list args;
    va_start(args, fmt);
    vsnprintf(current_error_message, sizeof(current_error_message), fmt, args);
    va_end(args);

    return 0;
}

int errmsg_internal(const char *fmt, ...) {
    if (!error_in_progress) {
        return 0;
    }

    va_list args;
    va_start(args, fmt);
    vsnprintf(current_error_message, sizeof(current_error_message), fmt, args);
    va_end(args);

    return 0;
}

int
pg_fprintf(FILE *stream, const char *fmt,...)
{
	va_list		args;

	va_start(args, fmt);
	vsnprintf(current_error_message, sizeof(current_error_message), fmt, args);
	va_end(args);
	return 0;
}


bool GetDefaultCharSignedness(void) {
    // XXX Stubbed for now
    return true;
}
