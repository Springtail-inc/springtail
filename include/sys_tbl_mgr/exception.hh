#pragma once

#include <common/exception.hh>

namespace springtail {

    /** parent connection error */
    class SysTblMgrError : public Error {
    public:
        SysTblMgrError() { }
        SysTblMgrError(const std::string &error)
            : Error(error)
        { }
    };

}
