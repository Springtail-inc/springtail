#include <backward.hpp>

// Sets up the signal handling to get backtrace prints on segfault and other terminating signals.
namespace backward {
    backward::SignalHandling sh;
}
