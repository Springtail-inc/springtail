#include <boost/program_options.hpp>

#include <common/init.hh>
#include <common/redis_types.hh>

#include <pg_fdw/pg_xid_collector_client.hh>

using namespace springtail;
using namespace springtail::pg_fdw;

int
main(int argc, char *argv[])
{
    uint64_t min_db_id;
    uint64_t max_db_id;
    uint64_t min_sleep_interval;
    uint64_t max_sleep_interval;
    uint64_t current_xid;

    // parse the arguments
    namespace po = boost::program_options;
    po::options_description desc("This process runs PgXidCollectorClient and uses it to issue periodic requests to PgXidCollector server process");
    desc.add_options()("help,h", "Help message.");
    desc.add_options()("min_db_id,md", po::value<uint64_t>(&min_db_id)->default_value(1), "Minimum Database ID");
    desc.add_options()("max_db_id,MD", po::value<uint64_t>(&max_db_id)->default_value(10), "Maximum Database ID");
    desc.add_options()("min_sleep,ms", po::value<uint64_t>(&min_sleep_interval)->default_value(10), "Minimum Sleep Interval in msec");
    desc.add_options()("max_max_sleep,MS", po::value<uint64_t>(&max_sleep_interval)->default_value(30'000), "Maximum Sleep Interval msec");
    desc.add_options()("xid,x", po::value<uint64_t>(&current_xid)->default_value(1), "Initial XID");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    springtail_init();

    std::jthread client_thread([&](std::stop_token st) {
        PgXidCollectorClient client;
        RedisClientPtr redis_client = RedisMgr::get_instance()->get_client();

        std::mt19937 gen(std::random_device{}());

        // Uniform in [min_db_id, max_db_id]
        std::uniform_int_distribution<int> uniform_dist(min_db_id, max_db_id);

        // get random database id
        int db_id = uniform_dist(gen);

        uint64_t db_instance_id = Properties::get_db_instance_id();
        std::string key = fmt::format(redis::SET_FDW_PID, db_instance_id);
        std::string value = fmt::format("{}:{}:{}", Properties::get_fdw_id(), db_id, getpid());
        CHECK(redis_client->sadd(key, value) == 1);

        // Skewed bell-shaped in [min_sleep_interval, max_sleep_interval], skewed toward max_sleep_interval
        double mean = min_sleep_interval + (max_sleep_interval - min_sleep_interval) * 0.005;
        double stddev = static_cast<double>(max_sleep_interval - min_sleep_interval) / 50;
        std::normal_distribution<double> normal_dist(mean, stddev);
        LOG_INFO("Created normal distribution with mean = {} and stddev = {}", mean, stddev);

        while (!st.stop_requested()) {
            // do work here

            // get random sleep interval that simulates random transaction duration
            uint64_t sleep_interval;
            do {
                sleep_interval = static_cast<uint64_t>(normal_dist(gen));
            } while (sleep_interval < min_sleep_interval || sleep_interval > max_sleep_interval);

            // send data to the server then sleep for a while
            client.send_data(db_id, current_xid);
            LOG_INFO("Sleep for {} milliseconds", sleep_interval);
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_interval));
            current_xid++;

            // get another db_id
            db_id = uniform_dist(gen);
        }
    });

    springtail_daemon_run();

    client_thread.request_stop();
    client_thread.join();

    springtail_shutdown();
    return 0;
}