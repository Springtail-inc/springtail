#pragma once

#include <string_view>
#include <memory>
#include <unordered_map>

#include <spdlog/pattern_formatter.h>

#include <opentelemetry/sdk/logs/logger_provider.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/trace/exporter.h>
#include <opentelemetry/sdk/trace/span_data.h>
#include <opentelemetry/trace/tracer.h>

#include <common/logging.hh>
#include <common/metric_constants.hh>
#include <common/singleton.hh>

namespace springtail::open_telemetry {
/** Convenience name */
using SpanPtr = opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>;

class OpenTelemetry : public Singleton<OpenTelemetry> {
    friend class Singleton<OpenTelemetry>;
public:
    /**
    * @brief Initialize the tracing system
    */
    void init(std::string_view component_name);

    static void flush();

    inline void
    log(const spdlog::source_loc &loc, const std::string &logger_name, spdlog::level::level_enum lvl, const std::string &formatted_msg)
    {
        if (!_inited_flag || _shutdown_flag || !(_otel_enabled && _otel_remote)) {
            return;
        }
        spdlog::details::log_msg message(loc, logger_name, lvl, formatted_msg);
        _log(message);
    }

    /**
     * @brief Get the context variables object
     *
     * @param callback - function called for each key/value pair
     * @return std::unordered_map<std::string, std::string> - map of current key/value pairs for the given scope
     */
    void
    get_context_variables(opentelemetry::nostd::function_ref<bool(opentelemetry::nostd::string_view, opentelemetry::nostd::string_view)> callback);

    /**
     * @brief Set the context variables object
     *
     * @param attributes - map of key/value pairs
     * @return std::unique_ptr<opentelemetry::context::Token> - scope token
     */
    std::unique_ptr<opentelemetry::context::Token>
    set_context_variables(const std::unordered_map<std::string, std::string>& attributes);

    /**
     * @brief Set the context variable object
     *
     * @param attr_key - varaible key
     * @param attr_value - variable value
     * @return std::unique_ptr<opentelemetry::context::Token> - scope token
     */
    std::unique_ptr<opentelemetry::context::Token>
    set_context_variable(const std::string &attr_key, const std::string &attr_value);

    /**
     * @brief Increment a counter
     * @param name The name of the counter
     * @param delta The increment delta
     */
     void increment_counter(std::string_view name, int delta=1);

     /**
      * @brief Record a value in the histogram
      * @param name The name of the histogram
      * @param value The value to record
      * @param attributes The attributes to record
      */
     void record_histogram(std::string_view name, double value);

     /**
     * @brief Retrieve the otel Tracer by name.
     */
     opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> tracer(const std::string_view& name);

private:
    OpenTelemetry() = default;      ///< default constructor
    ~OpenTelemetry() override = default;    ///< default destructor

    /**
     * @brief class shutdown function
     *
     */
    void _internal_shutdown() override;

    std::optional<std::string> _host;   ///< OTEL host to send data
    std::optional<int> _port;           ///< OTEL port to send data
    std::optional<int> _metrics_export_interval_millis; ///< metrics export interval
    std::optional<int> _metrics_export_timeout_millis;  ///< metrics export timeout
    std::string _remote_log_level;
    spdlog::level::level_enum _remote_log_level_value;
    bool _otel_enabled{false};      ///< OTEL enabled flag
    bool _otel_remote{false};       ///< OTEL remote enable flag
    static inline std::atomic<bool> _inited_flag{false};    ///< initialized flag
    static inline std::atomic<bool> _shutdown_flag{false};  ///< shutodwn flag

    /**
    * @brief A custom span exporter that logs OpenTelemetry spans using spdlog
    *
    * This exporter implements the OpenTelemetry SpanExporter interface to export
    * tracing spans by logging them through spdlog. It processes each span's name,
    * attributes, and other properties and outputs them as structured log messages.
    */
    class SpdlogExporter : public opentelemetry::sdk::trace::SpanExporter {
    public:
        std::unique_ptr<opentelemetry::sdk::trace::Recordable> MakeRecordable() noexcept override
        {
            return std::make_unique<opentelemetry::sdk::trace::SpanData>();
        }

        bool Shutdown(std::chrono::microseconds timeout) noexcept override { return true; }

        // Export a batch of spans (but we log each span individually in this example)
        opentelemetry::sdk::common::ExportResult Export(
            const opentelemetry::nostd::span<std::unique_ptr<opentelemetry::sdk::trace::Recordable>>&
                spans) noexcept override
        {
            for (const auto& span : spans) {
                const auto* span_data =
                    dynamic_cast<const opentelemetry::sdk::trace::SpanData*>(span.get());
                _log_span(*span_data);  // Log each span using spdlog
            }
            return opentelemetry::sdk::common::ExportResult::kSuccess;
        }

    private:
        void _log_span(const opentelemetry::sdk::trace::SpanData& span);
    };

    /**
    * @brief Map of counter names to their corresponding counters
    */
    std::map<std::string_view, opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Counter<uint64_t>>> _counters;

    /**
    * @brief Map of histogram names to their corresponding histograms
    */
    std::map<std::string_view, opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<double>>> _histograms;

    /**
     * @brief Meter provider object
     *
     */
    std::shared_ptr<opentelemetry::sdk::metrics::MeterProvider> _meter_provider;

    /**
     * @brief OTEL logger
     *
     */
     opentelemetry::nostd::shared_ptr<opentelemetry::logs::Logger> _logger;

     /**
      * @brief list of attribute key/value pairs that are al
      *
      */
    std::vector<std::pair<std::string, std::string>> _log_attributes;

    /**
     * @brief Formatter
     *
     */
    spdlog::pattern_formatter _formatter;

    void _init_metrics(const opentelemetry::sdk::resource::Resource& resource);
    void _init_tracing(const opentelemetry::sdk::resource::Resource& resource);
    void _init_logging(const opentelemetry::sdk::resource::Resource& resource);
    opentelemetry::sdk::resource::Resource _create_default_otel_resource(std::string_view component_name);

    /**
     * @brief Create a uint64 counter with the given name, description, and unit
     * @param name The name of the counter
     * @param description The description of the counter
     * @param unit The unit of the counter
     */
    opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Counter<uint64_t>>
    _create_uint64_counter(const std::string name, const std::string description, const std::string unit);

    /**
     * @brief Create a double histogram with the given name, description, and unit
     * @param name The name of the histogram
     * @param description The description of the histogram
     * @param unit The unit of the histogram
     */
    opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Histogram<double>>
    _create_double_histogram(const std::string name, const std::string description, const std::string unit);

    /**
     * @brief Register a counter with the given name, description, and unit
     * @param name The name of the counter
     * @param description The description of the counter
     * @param unit The unit of the counter
     */
    void _register_counter(std::string_view name, std::string_view description, std::string_view unit);

    /**
     * @brief Register a histogram with the given name, description, and unit
     * @param name The name of the histogram
     * @param description The description of the histogram
     * @param unit The unit of the histogram
     */
    void _register_histogram(std::string_view name, std::string_view description, std::string_view unit);

    void _log(const spdlog::details::log_msg &msg);
};

    /**
    * @brief This type delegates otel counter updates to a dedicated thread.
    * First define counter types with static OTel counter name methods:
    * struct MyCnt1 { static auto name() { return "otel counter 1"; } };
    * struct MyCnt2 { static auto name() { return "otel counter 2"; } };
    *
    * Instantiate this type:
    *
    * OTelCounters<MyCnt1, MyCnt2> counters;
    *
    * Increment the counters:
    *
    * counters.increment<MyCnt1>();
    * counters.increment<MyCnt2>();
    */
    template <typename... Names>
    struct OTelCounters 
    {
        using NamesTuple = std::tuple<Names...>;

        /** Constructor.
         * @param attrs Is a key-value map that is added to the metric OT context.
         * @param freq Defines the internal thread counter update frequency in sec.
         */
        OTelCounters(std::unordered_map<std::string, std::string> attrs, int freq_sec)
            :_attrs{std::move(attrs)},
            _freq_sec{freq_sec}
        {
            _t = std::make_unique<std::jthread>([this](std::stop_token st) { task(st); });
        }
        OTelCounters(OTelCounters&&) = delete;

        /** Increment the counter by one.
         */
        template<typename  Name>
        void increment()
        {
            // the static assert is just to make sure that the type
            // index is resolved at compile time. It doesn't do anything useful.
            static_assert(get_type_index<NamesTuple, Name>() <= 100000);
            ++std::get<get_type_index<NamesTuple, Name>()>(_counters);
        }

        template<typename  Name>
        int get() const {
            return std::get<get_type_index<NamesTuple, Name>()>(_counters).load();
        }
    
    private:
        std::unordered_map<std::string, std::string> _attrs;
        int _freq_sec;
        // counter increments
        std::array<std::atomic<int>, sizeof...(Names)> _counters;

        std::mutex _m;
        std::condition_variable_any _cv;
        std::unique_ptr<std::jthread> _t;


        // get index of the specific type in the tupple
        template <typename Tuple, typename T, size_t I = 0>
        static constexpr size_t get_type_index() {
            if constexpr (I == std::tuple_size_v<Tuple>) {
                static_assert(false);
            } else if constexpr (std::is_same_v<T, typename std::tuple_element<I, Tuple>::type>) {
                return I;
            } else {
                return get_type_index<Tuple, T, I+1>();
            }
        }

        template <size_t I>
        void update_counters() {
            if constexpr (I < sizeof...(Names)) {
                auto n = std::get<I>(_counters).load();
                if (n) {
                    OpenTelemetry::get_instance()->increment_counter(std::tuple_element<I, NamesTuple>::type::name(), n);
                    // reset the local counter
                    std::get<I>(_counters) = 0;
                }
                update_counters<I+1>();
            }
        }

        void task(std::stop_token st) {
            while(!st.stop_requested()) {
                std::unique_lock g(_m);
                if (_cv.wait_for(g, st, std::chrono::seconds(_freq_sec), 
                            [st]{ return !st.stop_requested(); }) ) {
                }
                auto token = OpenTelemetry::get_instance()->set_context_variables(_attrs);
                update_counters<0>();
            }
        }
    };


}  // namespace springtail::open_telemetry
