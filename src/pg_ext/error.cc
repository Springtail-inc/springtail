#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstring>

#include <pg_ext/error.hh>

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

    // if (!filename) filename = "<null>";
    // if (!funcname || (uintptr_t)funcname < 4096) funcname = "<invalid>";
    // if (!domain) domain = "<null>";

    // Log the error start with source location
    // fprintf(stderr, "Error started at %s:%d in %s (domain: %s)\n", filename, lineno, funcname, domain ? domain : "none");

    fprintf(stderr, "Error started at (domain: %s)\n", domain ? domain : "none");
    return true;
}

bool errstart_cold(int elevel, const char *domain) {
    // Cold path error start - same as regular errstart but optimized for cold path
    return errstart(elevel, domain);
}

void errfinish(int dummy, ...) {
    if (!error_in_progress) {
        return;
    }

    // Log the final error message
    if (current_error_message[0] != '\0') {
        switch (current_elevel) {
            case 20: // DEBUG5
            case 15: // DEBUG4
            case 14: // DEBUG3
            case 13: // DEBUG2
            case 12: // DEBUG1
                fprintf(stderr, "%s", current_error_message);
                break;
            case 10: // LOG
                fprintf(stderr, "%s", current_error_message);
                break;
            case 11: // COMMERROR
            case 8:  // WARNING
                fprintf(stderr, "%s", current_error_message);
                break;
            case 7:  // NOTICE
                fprintf(stderr, "%s", current_error_message);
                break;
            case 6:  // INFO
                fprintf(stderr, "%s", current_error_message);
                break;
            case 5:  // ERROR
                fprintf(stderr, "%s", current_error_message);
                break;
            case 4:  // FATAL
            case 3:  // PANIC
                fprintf(stderr, "%s", current_error_message);
                break;
            default:
                fprintf(stderr, "%s", current_error_message);
        }
    }

    // Reset error state
    error_in_progress = false;
    current_error_message[0] = '\0';
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
