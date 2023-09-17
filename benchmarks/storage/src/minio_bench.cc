#include <boost/program_options.hpp>
#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>

// MinIO client
#include <client.h>

/**
 * Timer class
 */
class Timer {
private:
    std::chrono::time_point<std::chrono::system_clock> _begin;
    std::chrono::time_point<std::chrono::system_clock> _end;

    std::chrono::milliseconds _total_elapsed;

public:
    Timer()
        : _total_elapsed(0)
    { }

    void
    start()
    {
        _begin = std::chrono::system_clock::now();
    }

    void
    stop()
    {
        _end = std::chrono::system_clock::now();
        _total_elapsed += std::chrono::duration_cast<std::chrono::milliseconds>(_end - begin).count();
    }

    std::chrono::milliseconds
    elapsed_ms()
    {
        return _total_elapsed;
    }
};


/**
 * Writer thread.
 */
void
writer(const std::string &url,
       const minio::creds::StaticProvider &provider,
       int file_name_start,
       int file_count,
       int file_size)
{
    // create a client for this thread
    minio::s3::Client client(url, provider);

    // create the objects
    std::vector<char> buf(buf_size);

    Timer timer;
    for (int i = 0; i < file_count; i++) {
        std::string object_name = fmt::format("{:08d}", file_name_start + i);

        // XXX fill the buffer with random data instead of zeros?

        minio::s3::PutObjectApiArgs args;
        args.bucket = "minio_bench";
        args.object = object_name;
        args.data = std::string_view(buf.data(), buf.size());

        // upload the object to minio
        timer.start()
        client.put_object(args);
        timer.stop()
    }

    std::cout << fmt::format("{:f}", timer.elapsed_ms() / file_count) << std::endl;
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

    // parse the credentials
    std::ostringstream ss;
    std::fstream file(credentials_file);
    ss << file.rdbuf(); // reading data
    std::string json_data = ss.str();

    const tao::json::value credentials = tao::json::from_string(json_data);


    // Create S3 base URL.
    minio::http::BaseUrl base_url;
    base_url.SetHost(credentials.at("url").get_string());
 
    // Create credential provider.
    minio::creds::StaticProvider provider(credentials.at("accessKey").get_string(),
                                          credentials.at("secretKey").get_string())
 
    // Create S3 client.
    minio::s3::Client client(base_url, &provider);

    std::string bucket_name = "minio_bench";
 
    // Check if bucket exists or not.
    bool exist;
    {
        minio::s3::BucketExistsArgs args;
        args.bucket_ = bucket_name;
 
        minio::s3::BucketExistsResponse resp = client.BucketExists(args);
        if (!resp) {
            std::cout << "unable to do bucket existence check; " << resp.GetError()
                      << std::endl;
            return EXIT_FAILURE;
        }
 
        exist = resp.exist_;
    }
 
    // Create bucket if it doesn't exist.
    if (!exist) {
        minio::s3::MakeBucketArgs args;
        args.bucket_ = bucket_name;
 
        minio::s3::MakeBucketResponse resp = client.MakeBucket(args);
        if (!resp) {
            std::cout << "unable to create bucket; " << resp.GetError() << std::endl;
            return EXIT_FAILURE;
        }
    }
 
    // start N writer threads, each writing X files of size Y
    //   create a file with zero data?  random data?
    //   pick a file name
    //   write the file (timed)
    std::vector<std::thread> writers;

    // start M reader threads, each reading Z random files
    //   pick a random file
    //   read the file (timed)
    std::vector<std::thread> readers;

    minio::s3::UploadObjectArgs args;
    args.bucket_ = bucket_name;
    args.object_ = "asiaphotos-2015.zip";
    args.filename_ = "/home/user/Photos/asiaphotos.zip";
 
    minio::s3::UploadObjectResponse resp = client.UploadObject(args);
    if (!resp) {
        std::cout << "unable to upload object; " << resp.GetError() << std::endl;
        return EXIT_FAILURE;
    }
 
    std::cout << "'/home/user/Photos/asiaphotos.zip' is successfully uploaded as "
              << "object 'asiaphotos-2015.zip' to bucket 'asiatrip'."
              << std::endl;
 
    return EXIT_SUCCESS;
}
