#include <sstream>

#include <common/time_trace.hh>

namespace springtail::time_trace {

time_trace::FlatTraceSet traces;

Trace&
FlatTraceSet::init(std::string_view name)
{
    auto it = std::ranges::find_if(traces, [&name](auto const& v) { return v.first == name; });
    CHECK(it == traces.end());
    List::value_type& a = traces.emplace_back(Item(name, Trace()));
    return a.second;
}

Trace&
FlatTraceSet::find(std::string_view name)
{
    auto it = std::ranges::find_if(traces, [&name](auto const& v) { return v.first == name; });
    return it->second;
}


void
FlatTraceSet::update(std::string_view name, const Trace& trace)
{
    auto it = std::ranges::find_if(traces, [&name](auto const& v) { return v.first == name; });
    if (it == traces.end()) {
        traces.emplace_back(Item(name, trace));
    } else {
        it->second += trace;
    }
}

void
FlatTraceSet::reset()
{
    for (auto& [_, item] : traces) {
        item.timer.reset();
        item.start_count = 0;
    }
}

std::string
FlatTraceSet::format()
{
    std::ostringstream s;

    s << "Time trace:";

    for (auto const& [name, item] : traces) {
        s << std::endl
          << "  " << name << "[total=" << item.timer.elapsed_ms()
          << ", counter=" << item.start_count
          << ", average=" << (item.timer.elapsed_ms() / item.start_count) << "]";
    }

    return s.str();
}

}  // namespace springtail::time_trace
