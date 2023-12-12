#include <grpc++/grpc++.h>
#include <flatbuffers/flatbuffers.h>

#include <write_cache/write_cache_fbs_generated.h>

namespace springtail {

}

int main (void)
{
    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:50051", grpc::InsecureServerCredentials());

}