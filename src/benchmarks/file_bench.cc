// external includes
#include <fcntl.h>

#include <fstream>
#include <iostream>
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
       int block_size)
{
    // create the files for this writer thread
    std::vector<int> handles;
    for (int i = 0; i < file_count; i++) {
        try {
            std::string file_name = fmt::format("{:03d}", file_name_start + i);

            // open a file handle
            int handle = ::open((directory / file_name).c_str(), O_CREAT | O_RDWR);
            handles.push_back(handle);
        } catch (std::exception &e) {
            std::cerr << "Error: " << e.what() << std::endl;
        }
    }

    // create a block of data
    std::vector<char> buf(block_size);

    springtail::common::Timer timer;
    timer.start();
    for (int i = 0; i < block_count; i++) {
        // round robin through the files
        int handle = handles[i % handles.size()];

        // append a block
        ::write(handle, buf.data(), buf.size());
    }
    timer.stop();

    std::cout << fmt::format("{:f}", (float)timer.elapsed_ms().count() / block_count) << std::endl;
}

/**
 * Reader thread.  Reads random blocks from the set of files.
 */
void
reader(const std::filesystem::path &directory,
       int file_count,
       int block_count,
       int block_size)
{
    // open handles to all of the files
    std::vector<int> handles, sizes;
    for (int i = 0; i < file_count; i++) {
        std::string file_name = fmt::format("{:03d}", i);

        // check the size of the file
        int file_size = std::filesystem::file_size(directory / file_name);
        sizes.push_back(file_size);

        // open a file handle
        int handle = ::open((directory / file_name).c_str(), O_RDONLY);
        handles.push_back(handle);

    }

    // create a block
    std::vector<char> buf(block_size);

    // read random blocks
    springtail::common::Timer timer;
    timer.start();
    for (int i = 0; i < block_count; i++) {
        int file = rand() % file_count;
        int pos = rand() % (sizes[file] / block_count);

        int handle = handles[file];
        ::lseek(handle, pos, SEEK_SET);
        ::read(handle, buf.data(), block_size);
    }
    timer.stop();

    std::cout << fmt::format("{:f}", (float)timer.elapsed_ms().count() / block_count) << std::endl;
}


/**
 * Benchmark for any filesystem
 */
int main(int argc, char* argv[]) {
    int block_count, block_size, file_count;
    int writer_count, reader_count;
    std::filesystem::path directory;

    // parse the arguments
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
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
    int files_per_thread = file_count / writer_count;
    int blocks_per_thread = block_count / writer_count;
    springtail::common::Timer timer;

    timer.start();
    for (int i = 0; i < writer_count; i++) {
        writers.push_back(std::thread(writer,
                                      directory,
                                      i * files_per_thread,
                                      files_per_thread,
                                      blocks_per_thread,
                                      block_size));
    }
    for (auto &&w : writers) {
        w.join();
    }
    timer.stop();
    std::cout << "total writer time: " << fmt::format("{:f}", (float)timer.elapsed_ms().count()) << std::endl;

    std::cout << "about to start readers" << std::endl;

    // start M reader threads, each reading Z random files
    //   pick a random file
    //   read the file (timed)
    timer.reset();
    timer.start();
    std::vector<std::thread> readers;
    for (int i = 0; i < writer_count; i++) {
        readers.push_back(std::thread(reader,
                                      directory,
                                      file_count,
                                      block_count,
                                      block_size));
    }
    for (auto &&r : readers) {
        r.join();
    }
    timer.stop();
    std::cout << "total reader time: " << fmt::format("{:f}", (float)timer.elapsed_ms().count()) << std::endl;

    // cleanup
    std::cout << "Start cleanup" << std::endl;
    std::filesystem::remove_all(directory);
    std::cout << "Benchmark complete" << std::endl;

    return 0;
}
