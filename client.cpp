#include <gflags/gflags.h>
#include <butil/logging.h>
#include <butil/time.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <braft/raft.h>
#include "payload.pb.h"

DEFINE_string(addr, "127.0.0.1:8100", "Server address");
DEFINE_int32(timeout_ms, 1000, "RPC timeout in milliseconds");
DEFINE_int32(request_size, 256, "Size of the payload to send");

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    
    // Initialize channel
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.connection_type = "single";
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = 3;

    if (channel.Init(FLAGS_addr.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }

    example::PayloadService_Stub stub(&channel);
    
    // Prepare payload
    std::string payload(FLAGS_request_size, 'x');  // Fill with 'x' characters
    example::PayloadRequest request;
    request.set_payload(payload);
    
    // Send request
    example::PayloadResponse response;
    brpc::Controller cntl;
    stub.replicate_payload(&cntl, &request, &response, NULL);
    
    if (cntl.Failed()) {
        LOG(ERROR) << "Fail to send request to " << FLAGS_addr 
                  << ": " << cntl.ErrorText();
        return -1;
    }
    
    if (!response.success()) {
        if (response.has_redirect()) {
            LOG(ERROR) << "Not leader, redirect to " << response.redirect();
        } else {
            LOG(ERROR) << "Failed to replicate payload";
        }
        return -1;
    }
    
    LOG(INFO) << "Replicated payload successfully, size=" 
              << response.payload().size();
    return 0;
} 