#include <boost/program_options.hpp>

// springtail includes
#include <common/common.hh>
#include <pg_log_mgr/pg_log_mgr.hh>

namespace {
    std::shared_ptr<springtail::PgLogMgr> log_mgr;

    void
    handle_sigint(int signal)
    {
        log_mgr->shutdown();
    }
}

int main(int argc, char *argv[])
{
    std::string host;
    std::string db_name;
    std::string user_name;
    std::string password;
    std::string pub_name;
    std::string slot_name;
    std::filesystem::path repl_log_path;
    std::filesystem::path xact_log_path;

    //bool create_slot = false;
    int port;

    // parse the arguments
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("host,H", boost::program_options::value<std::string>(&host)->default_value("localhost"), "Hostname")
        ("port,p", boost::program_options::value<int>(&port)->default_value(5432), "Port number")
        ("dbname,d", boost::program_options::value<std::string>(&db_name)->default_value("springtail"), "DB database name")
        ("user,u", boost::program_options::value<std::string>(&user_name)->default_value("springtail"), "DB user name")
        ("password,P", boost::program_options::value<std::string>(&password)->default_value(""), "DB Password")
        ("publication,b", boost::program_options::value<std::string>(&pub_name)->default_value("springtail"), "Publication name")
        ("slot,s", boost::program_options::value<std::string>(&slot_name)->default_value("springtail"), "Slot name; if none specified slot will be created")
        ("repl_path,r", boost::program_options::value<std::filesystem::path>(&repl_log_path)->default_value(std::filesystem::path("./repl_logs")), "Replication log file path")
        ("xact_path,x", boost::program_options::value<std::filesystem::path>(&xact_log_path)->default_value(std::filesystem::path("./xact_logs")), "Transaction log file path")
        ("daemonize", "Start the server as a daemon")
    ;

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }
    boost::program_options::notify(vm);

    // daemonize the process
    if (vm.count("daemonize")) {
        common::daemonize("/tmp/write_cache.pid");
    }

    springtail::springtail_init();

    log_mgr = std::make_shared<springtail::PgLogMgr>(repl_log_path, xact_log_path, host,
                                                     db_name, user_name, password, pub_name,
                                                     slot_name, port);

    // register the SIGINT handler
    std::signal(SIGINT, handle_sigint);

    log_mgr->start_streaming();

    log_mgr->join();

    return 0;
}
