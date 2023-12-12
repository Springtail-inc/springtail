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

    public:
        GrpcChannelPool(const std::string &server_addr, int start_channels, int max_channels)
            : _server_addr(server_addr), _max_channels(max_channels)
        {
            // initialize queue with starting set of channels
            for (int i = 0; i < start_channels; i++) {
                _queue.push(grpc::CreateChannel(_server_addr, grpc::InsecureChannelCredentials()));
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
                std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(
                    _server_addr,
                    grpc::InsecureChannelCredentials()
                );
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

    template <class StubClass>
    class GrpcClientContext 
    {
    public:
        GrpcClientContext(std::shared_ptr<GrpcChannelPool> channel_pool) 
            : _channel_pool(channel_pool)
        {
            _channel = _channel_pool->get_channel(); // may block
            _stub = std::make_shared<StubClass>(_channel);
        }

        ~GrpcClientContext()
        {
            _channel_pool->put_channel(_channel);
            _channel = nullptr;
            _stub = nullptr;
        }

        std::shared_ptr<StubClass> stub() {
            return _stub;
        }    
    
    private:
        std::shared_ptr<StubClass> _stub;
        std::shared_ptr<grpc::Channel> _channel;
        std::shared_ptr<GrpcChannelPool> _channel_pool;
    };


    template <class StubClass>
    class GrpcClient
    {
    public:
        GrpcClient(const std::string &server_addr, int start_channels, int max_channels) 
        { 
            _channel_pool = std::make_shared<GrpcChannelPool>(server_addr, start_channels, max_channels);
        }
    
    
        std::unique_ptr<GrpcClientContext<StubClass>> make_stub() {
            return std::make_unique<GrpcClientContext<StubClass>>(_channel_pool);
        }
    
    private:
        std::shared_ptr<GrpcChannelPool> _channel_pool;

        void _stub_deleter(StubClass *ptr) { delete ptr; }
    };
}