#include <iostream>
#include <string>
#include <thread>

#include <fmt/core.h>

#include <boost/asio.hpp>

#include <boost/program_options.hpp>

void client(int port, int count, int id)
{
    std::string host = "127.0.0.1";

    boost::asio::io_service io_service;

    //socket creation
    boost::asio::ip::tcp::socket socket(io_service);

    //connection
    std::cout << "Thread connecting to " << host << ":" << port << std::endl;
    socket.connect(boost::asio::ip::tcp::endpoint(boost::asio::ip::address::from_string(host), port));

    // request/message from client
    const std::string msg = fmt::format("Hello from Client: {}", id);
    boost::system::error_code error;

    for (int i = 0; i < count; i++) {
        boost::asio::write(socket, boost::asio::buffer(msg), error);
        if (!error) {
            std::cout << "Client "<< id << " sent: " << msg << std::endl;
        } else {
            std::cout << "send failed: " << error.message() << std::endl;
            break;
        }

        // getting response from server
        boost::asio::streambuf receive_buffer;
        boost::asio::read(socket, receive_buffer, boost::asio::transfer_at_least(1), error);
        if (error && error != boost::asio::error::eof) {
            std::cout << "receive failed: " << error.message() << std::endl;
            break;
        } else {
            const char* data = boost::asio::buffer_cast<const char*>(receive_buffer.data());
            std::cout << "Client " << id << " received: " << data << std::endl;
        }
    }
}

int main(int argc, char* argv[])
{
    int port;
    int num_threads;
    int msg_count;

    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("port,p", boost::program_options::value<int>(&port)->default_value(8888), "Port number")
        ("threads,t", boost::program_options::value<int>(&num_threads)->default_value(4), "Number of threads")
        ("count,c", boost::program_options::value<int>(&msg_count)->default_value(4), "Msgs per thread");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    // create threads
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; i++) {
        threads.push_back(std::thread(client, port, msg_count, i));
    }

    // wait for threads to complete
    for (int i = 0; i < num_threads; i++) {
        threads[i].join();
    }

    return 0;
}