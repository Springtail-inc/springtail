// external includes
#include <fstream>
#include <iostream>
#include <thread>

#include <boost/program_options.hpp>
#include <fmt/core.h>
#include <nlohmann/json.hpp>
#include <miniocpp/client.h>

// springtail includes
#include <common/timer.hh>

/**
 * Writer thread.
 */
void
writer(const std::string &url,
       const std::string &access_key,
       const std::string &secret_key,
       int file_name_start,
       int file_count,
       int file_size)
{
    // create a client for this thread
    minio::s3::BaseUrl base_url(url);
    minio::creds::StaticProvider provider(access_key, secret_key);
    minio::s3::Client client(base_url, &provider);

    // create the objects
    std::vector<char> buf(file_size);

    springtail::Timer timer;
    for (int i = 0; i < file_count; i++) {
        std::string object_name = fmt::format("{:08d}", file_name_start + i);

        // XXX fill the buffer with random data instead of zeros?
        std::stringstream stream(std::string(buf.begin(), buf.end()));

        // minio::s3::PutObjectApiArgs args;
        // args.bucket = "minio_bench";
        // args.object = object_name;
        // args.data = std::string_view(buf.data(), buf.size());

        minio::s3::PutObjectArgs args(stream, file_size, 0);
        args.bucket = "minio_bench";
        args.object = object_name;

        // upload the object to minio
        timer.start();
        client.PutObject(args);
        timer.stop();
    }

    std::cout << fmt::format("{:f}", timer.elapsed_ms().count() / file_count) << std::endl;
}

/**
 * Reader thread.
 */
void
reader()
{

}

/**
 * Benchmark for MinIO
 */
int main(int argc, char* argv[]) {
    std::string credentials_file;

    // parse the arguments
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "Help message.")
        ("creds,c", boost::program_options::value<std::string>(&credentials_file)->default_value("credentials.json"), "MinIO credentials file.");
    // XXX number of files
    // XXX size of files
    // XXX number of concurrent threads

    boost::program_options::variables_map vm;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
    boost::program_options::notify(vm);

    // check if we need to print the help message
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    // parse the credentials
    std::ostringstream ss;
    std::fstream file(credentials_file);
    ss << file.rdbuf(); // reading data
    std::string json_data = ss.str();

    const nlohmann::json credentials = nlohmann::json::parse(json_data);


    // Create S3 base URL.
    minio::s3::BaseUrl base_url(credentials.at("url").get<std::string>());
 
    // Create credential provider.
    minio::creds::StaticProvider provider(credentials.at("accessKey").get<std::string>(),
                                          credentials.at("secretKey").get<std::string>());
 
    // Create S3 client.
    minio::s3::Client client(base_url, &provider);

    std::string bucket_name = "miniobench";
 
    // Check if bucket exists or not.
    bool exist;
    {
        minio::s3::BucketExistsArgs args;
        args.bucket = bucket_name;
        args.region = "us-east-1";
 
        minio::s3::BucketExistsResponse resp = client.BucketExists(args);
        if (!resp) {
            std::cout << "unable to do bucket existence check; " << resp.Error()
                      << std::endl;
            return -1;
        }

        std::cout << "message: " << resp.message << std::endl;
 
        exist = resp.exist;
    }
 
    // Create bucket if it doesn't exist.
    if (!exist) {
        minio::s3::MakeBucketArgs args;
        args.bucket = bucket_name;
 
        minio::s3::MakeBucketResponse resp = client.MakeBucket(args);
        std::cout << "message: " << resp.message << std::endl;
        if (!resp) {
            std::cout << "unable to create bucket; " << resp.Error() << std::endl;
            return -1;
        }
    }

    std::cout << "about to start writers" << std::endl;

    // start N writer threads, each writing X files of size Y
    //   create a file with zero data?  random data?
    //   pick a file name
    //   write the file (timed)
    std::vector<std::thread> writers;

    // start M reader threads, each reading Z random files
    //   pick a random file
    //   read the file (timed)
    std::vector<std::thread> readers;

    return 0;
}
