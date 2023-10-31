// external includes
#include <fcntl.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <numeric>
#include <thread>

#include <boost/program_options.hpp>
#include <fmt/core.h>

// springtail includes
#include <common/timer.hh>


/**
 * Writer thread.  Writes blocks to files, append only.
 */
void
writer(const std::filesystem::path &directory,
       int file_name_start,
       int file_count,
       int block_count,
       int block_size,
       int &iops)
{
    // create the files for this writer thread
    std::vector<int> handles;
    for (int i = 0; i < file_count; i++) {
        try {
            std::string file_name = fmt::format("{:03d}", file_name_start + i);

            // open a file handle
            int handle = ::open((directory / file_name).c_str(), O_CREAT | O_RDWR, 00666);
            if (handle < 0) {
                std::cerr << "ERROR: creating file" << std::endl;
            }
            handles.push_back(handle);
        } catch (std::exception &e) {
            std::cerr << "Error: " << e.what() << std::endl;
        }
    }

    // create a block of data
    std::vector<char> buf(block_size);

    springtail::Timer timer;
    timer.start();
    for (int i = 0; i < block_count; i++) {
        // round robin through the files
        int handle = handles[i % handles.size()];

        // append a block
        int count = ::write(handle, buf.data(), buf.size());
        if (count < 0) {
            std::cerr << "ERROR: writing to file" << std::endl;
        }
    }
    timer.stop();

    // close the file
    // close the handles
    for (int handle : handles) {
        ::close(handle);
    }

    iops = static_cast<int>(static_cast<float>(block_count * 1000) / (static_cast<float>(timer.elapsed_ms().count())));
    std::cout << fmt::format("Writer thread IOPS: {:d}", iops) << std::endl;
}

/**
 * Reader thread.  Reads random blocks from the set of files.
 */
void
reader(const std::filesystem::path &directory,
       int file_count,
       int block_count,
       int block_size,
       int &iops)
{
    // open handles to all of the files
    std::vector<int> handles;
    std::vector<uint64_t> sizes;
    for (int i = 0; i < file_count; i++) {
        std::string file_name = fmt::format("{:03d}", i);

        // check the size of the file
        uint64_t file_size = std::filesystem::file_size(directory / file_name);
        sizes.push_back(file_size);

        // open a file handle
        int handle = ::open((directory / file_name).c_str(), O_RDONLY);
        if (handle < 0) {
            std::cerr << "ERROR: opening file for read" << std::endl;
        }
        handles.push_back(handle);
    }

    // create a block
    std::vector<char> buf(block_size);

    // read random blocks
    springtail::Timer timer;
    timer.start();
    for (int i = 0; i < block_count; i++) {
        int file = rand() % file_count;
        uint64_t pos = rand() % (sizes[file] / block_size);

        int handle = handles[file];
        ::lseek(handle, pos * block_size, SEEK_SET);
        int count = ::read(handle, buf.data(), block_size);
        if (count < 0) {
            std::cerr << "ERROR: reading from file" << std::endl;
        }
    }
    timer.stop();

    // close the handles
    for (int handle : handles) {
        ::close(handle);
    }

    iops = static_cast<int>(static_cast<float>(block_count * 1000) / static_cast<float>(timer.elapsed_ms().count()));
    std::cout << fmt::format("Reader thread IOPS: {:d}", iops) << std::endl;
}


void
concurrent_setup(const std::filesystem::path &directory,
                 int block_count,
                 int block_size)
{
    int handle;
    try {
        std::string file_name = "1";

        // open a file handle
        handle = ::open((directory / file_name).c_str(), O_CREAT | O_RDWR, 00666);
        if (handle < 0) {
            std::cerr << "Error opening: " << handle << std::endl;
        }
    } catch (std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    // create a block of data
    std::vector<char> buf(block_size);

    springtail::Timer timer;
    timer.start();
    for (int i = 0; i < block_count; i++) {
        // append a block
        int count = ::write(handle, buf.data(), buf.size());
        if (count < 0) {
            std::cerr << "Error writing: " << count << std::endl;
        }
    }
    timer.stop();

    // close the handle
    ::close(handle);

    int iops = static_cast<int>(static_cast<float>(block_count * 1000) / (static_cast<float>(timer.elapsed_ms().count())));
    std::cout << fmt::format("Setup IOPS: {:d}", iops) << std::endl;
}

void
concurrent_writer(const std::filesystem::path &directory,
                  int block_count,
                  int block_size,
                  int &iops)
{
    int handle;
    try {
        std::string file_name = "1";

        // open a file handle
        handle = ::open((directory / file_name).c_str(), O_APPEND | O_RDWR);
        if (handle < 0) {
            std::cerr << "ERROR: opening file for append" << std::endl;
        }
        ::lseek(handle, 0, SEEK_END);
    } catch (std::exception &e) {
        std::cerr << "ERROR: " << e.what() << std::endl;
    }

    // create a block of data
    std::vector<char> buf(block_size);

    std::cout << "Start writer" << std::endl;
    springtail::Timer timer;
    timer.start();
    for (int i = 0; i < block_count; i++) {
        // append a block
        int count = ::write(handle, buf.data(), buf.size());
        if (count < 0) {
            std::cerr << "ERROR: writing to file" << std::endl;
        }
    }
    timer.stop();
    std::cout << "Stop writer" << std::endl;

    iops = static_cast<int>(static_cast<float>(block_count * 1000) / static_cast<float>(timer.elapsed_ms().count()));
    ::close(handle);
}


void
concurrent_reader(const std::filesystem::path &directory,
                  int block_count,
                  int block_size,
                  int &iops)
{
    int handle;

    try {
        std::string file_name = "1";

        // open a file handle
        handle = ::open((directory / file_name).c_str(), O_RDONLY);
        if (handle < 0) {
            std::cerr << "ERROR: opening file for read" << std::endl;
        }
    } catch (std::exception &e) {
        std::cerr << "ERROR: " << e.what() << std::endl;
    }

    // create a block
    std::vector<char> buf(block_size);

    // read random blocks
    std::cout << "Start reader" << std::endl;
    springtail::Timer timer;
    timer.start();
    for (int i = 0; i < block_count; i++) {
        uint64_t pos = rand() % block_count;

        ::lseek(handle, pos * block_size, SEEK_SET);
        int count = ::read(handle, buf.data(), block_size);
        if (count < 0) {
            std::cerr << "ERROR: reading file" << std::endl;
        }
    }
    timer.stop();

    // close the handle
    std::cout << "Stop reader" << std::endl;
    ::close(handle);

    iops = static_cast<int>(static_cast<float>(block_count * 1000) / static_cast<float>(timer.elapsed_ms().count()));
    std::cout << fmt::format("Reader thread IOPS: {:d}", iops) << std::endl;
}

/**
 * Benchmark for any filesystem
 */
int main(int argc, char* argv[]) {
    bool run_concurrent;
    int block_count, block_size, file_count;
    int writer_count, reader_count;
    std::filesystem::path directory;

    // parse the arguments
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("concurrent,x", boost::program_options::value<bool>(&run_concurrent)->default_value(false), "Run the concurrent read/write test")
        ("count,n", boost::program_options::value<int>(&block_count)->default_value(1024*1024), "Number of blocks to write")
        ("size,s", boost::program_options::value<int>(&block_size)->default_value(64*1024), "Size of a block")
        ("files,f", boost::program_options::value<int>(&file_count)->default_value(16), "Number of files to distribute the blocks over")
        ("writers,w", boost::program_options::value<int>(&writer_count)->default_value(4), "Number of writer threads")
        ("readers,r", boost::program_options::value<int>(&reader_count)->default_value(4), "Number of reader threads")
        ("dir,d", boost::program_options::value<std::filesystem::path>(&directory)->default_value("benchmark"), "Directory to store files in");

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }


    // create a directory for the benchmark
    std::filesystem::create_directories(directory);

    std::cout << "about to start writers" << std::endl;

    // start N writer threads, each writing X files of size Y
    //   create a file with zero data?  random data?
    //   pick a file name
    //   write the file (timed)
    std::vector<std::thread> writers;
    std::vector<int> writer_iops(writer_count);
    int files_per_thread = file_count / writer_count;
    int blocks_per_thread = block_count / writer_count;
    springtail::Timer timer;

    timer.start();
    for (int i = 0; i < writer_count; i++) {
        writers.push_back(std::thread(writer,
                                      directory,
                                      i * files_per_thread,
                                      files_per_thread,
                                      blocks_per_thread,
                                      block_size,
                                      std::ref(writer_iops[i])));
    }
    for (auto &&w : writers) {
        w.join();
    }
    timer.stop();

    std::cout << "total writer time: " << fmt::format("{:d}", (int)timer.elapsed_ms().count()) << std::endl;
    std::cout << fmt::format("total writer iops: {:d}", std::reduce(writer_iops.begin(), writer_iops.end())) << std::endl;

    std::cout << "about to start readers" << std::endl;

    // start M reader threads, each reading Z random files
    //   pick a random file
    //   read the file (timed)
    timer.reset();
    timer.start();
    std::vector<std::thread> readers;
    std::vector<int> reader_iops(writer_count);
    for (int i = 0; i < reader_count; i++) {
        readers.push_back(std::thread(reader,
                                      directory,
                                      file_count,
                                      block_count,
                                      block_size,
                                      std::ref(reader_iops[i])));
    }
    for (auto &&r : readers) {
        r.join();
    }
    timer.stop();

    std::cout << "total reader time: " << fmt::format("{:d}", (int)timer.elapsed_ms().count()) << std::endl;
    std::cout << fmt::format("total reader iops: {:d}", std::reduce(reader_iops.begin(), reader_iops.end())) << std::endl;


    // cleanup
    std::cout << "Start cleanup" << std::endl;
    std::filesystem::remove_all(directory);
    std::cout << "Benchmark complete" << std::endl;

    if (run_concurrent) {
        // Evaluate doing reads while concurrently appending to a file to understand locking behaviors
        timer.reset();
        timer.start();

        std::filesystem::create_directories(directory);

        // create a file with some data in it
        concurrent_setup(directory, block_count, block_size);

        // concurrent writer thread
        int cwriter_iops;
        std::thread cwriter(concurrent_writer,
                            directory,
                            block_count,
                            block_size,
                            std::ref(cwriter_iops));

        // concurrent reader thread
        int creader_iops;
        std::thread creader(concurrent_reader,
                            directory,
                            block_count,
                            block_size,
                            std::ref(creader_iops));

        // wait for writer and reader to complete
        cwriter.join();
        creader.join();

        timer.stop();

        // report on writer and reader IOPS
        std::cout << fmt::format("concurrent writer iops: {:d}", cwriter_iops) << std::endl;
        std::cout << fmt::format("concurrent reader iops: {:d}", creader_iops) << std::endl;

        std::cout << "Start cleanup" << std::endl;
        std::filesystem::remove_all(directory);
        std::cout << "Benchmark complete" << std::endl;
    }

    return 0;
}
