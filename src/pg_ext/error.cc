#include <cstdarg>
#include <cstdio>
#include <cstring>

#include <common/logging.hh>
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
        LOG_WARN("Process interrupt received");
    }
}

bool errstart(int elevel, const char *filename, int lineno, const char *funcname, const char *domain) {
    // Reset error state
    current_elevel = elevel;
    current_sqlcode = 0;
    current_error_message[0] = '\0';
    error_in_progress = true;

    // Log the error start with source location
    LOG_DEBUG(springtail::LOG_COMMON, "Error started at {}:{} in {} (domain: {})", 
              filename, lineno, funcname, domain ? domain : "none");

    return true;
}

bool errstart_cold(int elevel, const char *filename, int lineno, const char *funcname, const char *domain) {
    // Cold path error start - same as regular errstart but optimized for cold path
    return errstart(elevel, filename, lineno, funcname, domain);
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
                LOG_DEBUG(springtail::LOG_COMMON, "{}", current_error_message);
                break;
            case 10: // LOG
                LOG_INFO("{}", current_error_message);
                break;
            case 11: // COMMERROR
            case 8:  // WARNING
                LOG_WARN("{}", current_error_message);
                break;
            case 7:  // NOTICE
                LOG_INFO("{}", current_error_message);
                break;
            case 6:  // INFO
                LOG_INFO("{}", current_error_message);
                break;
            case 5:  // ERROR
                LOG_ERROR("{}", current_error_message);
                break;
            case 4:  // FATAL
            case 3:  // PANIC
                LOG_CRITICAL("{}", current_error_message);
                break;
            default:
                LOG_INFO("{}", current_error_message);
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

