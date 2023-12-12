#include <memory>
#include <string>
#include <mutex>
#include <queue>
#include <condition_variable>

#include <grpc++/grpc++.h>

namespace springtail {

class GrpcChannelPool {
    private:
        std::string _server_addr;
        std::mutex _mutex;
        std::condition_variable _cv;
        std::queue<std::shared_ptr<grpc::Channel>> _queue;
        int _outstanding_channels=0;
        int _max_channels;
        bool _waiting=false;

        std::shared_ptr<grpc::Channel> _create_channel(int id)
        {
            grpc::ChannelArguments args;
            // Set a dummy (but distinct) channel arg on each channel so that every
            // channel gets its own connection; see https://github.com/grpc/grpc/issues/15535
            args.SetInt("id_key", id);
            return grpc::CreateCustomChannel(_server_addr,
                                             grpc::InsecureChannelCredentials(),
                                             args);
        }

    public:
        GrpcChannelPool(const std::string &server_addr, int start_channels, int max_channels)
            : _server_addr(server_addr), _max_channels(max_channels)
        {
            // initialize queue with starting set of channels
            for (int i = 0; i < start_channels; i++) {
                _queue.push(_create_channel(i));
            }
        }

        std::shared_ptr<grpc::Channel> get_channel()
        {
            std::unique_lock<std::mutex> queue_lock(_mutex);
            
            while (_queue.empty() && _outstanding_channels >= _max_channels) {
                _waiting = true;
                _cv.wait(queue_lock);
            }

            if (_queue.empty() && _outstanding_channels < _max_channels) {
                std::shared_ptr<grpc::Channel> channel = _create_channel(_outstanding_channels);
                _outstanding_channels++;
                return channel;
            }

            std::shared_ptr<grpc::Channel> channel = _queue.front();
            _queue.pop();
            _outstanding_channels++;
            return channel;
        }

        void put_channel(std::shared_ptr<grpc::Channel> channel)
        {
            std::unique_lock<std::mutex> queue_lock(_mutex);
            _queue.push(channel);
            _outstanding_channels--;
            if (_waiting) {
                _cv.notify_one();
            }
        }
    };
}