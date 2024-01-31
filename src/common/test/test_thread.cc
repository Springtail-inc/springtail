#include <common/thread_pool.hh>

#include <iostream>

class TestRequest {
public:
    int _a;
    TestRequest(int a) : _a(a) {}
    void operator()() {
        std::cout << "Request is working..." << _a << "\n";
    }
};


int main(void)
{
    springtail::ThreadPool<TestRequest> pool(2);

    pool.queue(std::make_shared<TestRequest>(1));
    pool.queue(std::make_shared<TestRequest>(2));
    pool.queue(std::make_shared<TestRequest>(3));
    pool.queue(std::make_shared<TestRequest>(4));

    pool.shutdown();    
}