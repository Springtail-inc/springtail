#include <sstream>
#include <common/time_trace.hh>

namespace springtail 
{ 
namespace time_trace
{

void 
FlatTrace::start(Name name) 
{
    auto it = std::ranges::find_if(trace,
            [&name](auto const& v) { return v.first == name; });
    if (it != trace.end()) {
        it->second.timer.start();
        ++(it->second.start_count);
    } else {
        Trace t;
        t.timer.start();
        ++t.start_count;
        trace.emplace_back(std::move(name), t);
    }
}

void 
FlatTrace::stop(const Name& name)
{
    auto it = std::ranges::find_if(trace,
            [&name](auto const& v) { return v.first == name; });
    CHECK(it != trace.end());
    it->second.timer.stop();
}

void FlatTrace::reset()
{
    for( auto & [_, item]: trace) {
        item.timer.reset();
        item.start_count = 0;
    }
}

std::string 
FlatTrace::format() 
{
    std::ostringstream s;

    s << "Time trace:";

    for (auto const& [name, item]: trace) {
        s << std::endl << "  " << name 
            << "[total=" << item.timer.elapsed_ms() 
            << ", counter=" << item.start_count
            << ", average=" << (item.timer.elapsed_ms()/item.start_count) << "]";
    }

    return s.str();
}

}
}
