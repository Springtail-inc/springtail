#include <common/common.hh>
#include <common/exception.hh>
#include <common/logging.hh>

namespace springtail {
    void springtail_init() {
        // initialize the backtrace infrastructure
        backward::sh.loaded();

        // initialize the logging infrastructure
        init_logging();
    }
}
