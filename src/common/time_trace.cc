#include <sstream>
#include <common/time_trace.hh>

namespace springtail 
{ 
namespace time_trace
{

void 
FlatTrace::start(Name name) 
{
    auto it = std::ranges::find_if(_trace,
            [&name](auto const& v) { return v.first == name; });
    if (it != _trace.end()) {
        CHECK(!it->second._started);
        it->second._timer.start();
        ++(it->second._start_count);
        it->second._started = true;
    } else {
        Trace t;
        t._timer.start();
        ++t._start_count;
        t._started = true;
        _trace.emplace_back(std::move(name), t);
    }
}

void 
FlatTrace::stop(const Name& name)
{
    auto it = std::ranges::find_if(_trace,
            [&name](auto const& v) { return v.first == name; });
    CHECK(it != _trace.end());
    CHECK(it->second._started);
    it->second._started = false;
    it->second._timer.stop();
}

void FlatTrace::reset()
{
    for( auto & [_, item]: _trace) {
        item._timer.reset();
        item._started = false;
        item._start_count = 0;
    }
}

std::string 
FlatTrace::format() 
{
    std::ostringstream s;

    s << "Time trace:";

    for (auto const& [name, item]: _trace) {
        s << std::endl << "  " << name 
            << "[total=" << item._timer.elapsed_ms() 
            << ", counter=" << item._start_count
            << ", average=" << (item._timer.elapsed_ms()/item._start_count) << "]";
    }

    return s.str();
}

}
}
