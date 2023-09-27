#pragma once


namespace st_common {
    /** 
     * Base error class for exceptions.  When in debug mode uses cpptrace::exception to capture stack traces.
     */
#ifdef NDEBUG
    typedef std::exception Error;
#else
    typedef cpptrace::exception Error;
#endif
}
