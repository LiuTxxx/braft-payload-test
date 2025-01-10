// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
DEFINE_bool(use_bthread, false, "Use bthread to send requests");
DEFINE_int32(thread_num, 1, "Number of threads sending requests");
DEFINE_int32(send_iters, 0, "Number of requests to send for each thread, 0 means unlimited");
DEFINE_int32(send_interval_ms, 1000, "Milliseconds between consecutive requests");

bvar::LatencyRecorder g_latency_recorder("client");
bvar::Adder<int> g_error_counter("client_error_count");

static void* sender(void* arg) {
    int thread_index = (int)(intptr_t)arg;
    int base_counter = thread_index << 24;
    int counter = 0;
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.connection_type = "single";
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = 3;

    if (channel.Init(FLAGS_addr.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return NULL;
    }

    example::PayloadService_Stub stub(&channel);

    while (!brpc::IsAskedToQuit() && 
           (FLAGS_send_iters == 0 || counter < FLAGS_send_iters)) {
        example::PayloadRequest request;
        example::PayloadResponse response;
        brpc::Controller cntl;

        request.set_payload(std::string(FLAGS_request_size, 'x'));
        
        stub.replicate_payload(&cntl, &request, &response, NULL);
        if (!cntl.Failed()) {
            g_latency_recorder << cntl.latency_us();
            if (response.success()) {
                ++counter;
                continue;
            }
            if (response.has_redirect()) {
                LOG(WARNING) << "Redirected to " << response.redirect();
                if (channel.Init(response.redirect().c_str(), &options) != 0) {
                    LOG(ERROR) << "Fail to initialize channel to " << response.redirect();
                }
                continue;
            }
        } else {
            g_error_counter << 1;
        }
        LOG_EVERY_SECOND(WARNING) << "Fail to send request to " << FLAGS_addr
                                 << " : " << (cntl.Failed() ? cntl.ErrorText() : "redirect");
        bthread_usleep(FLAGS_timeout_ms * 1000L);
    }

    return NULL;
}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    std::vector<bthread_t> tids;
    std::vector<pthread_t> pids;
    if (!FLAGS_use_bthread) {
        pids.resize(FLAGS_thread_num);
        for (int i = 0; i < FLAGS_thread_num; ++i) {
            if (pthread_create(&pids[i], NULL, sender, (void*)(intptr_t)i) != 0) {
                LOG(ERROR) << "Fail to create pthread";
                return -1;
            }
        }
    } else {
        tids.resize(FLAGS_thread_num);
        for (int i = 0; i < FLAGS_thread_num; ++i) {
            if (bthread_start_background(&tids[i], NULL, sender, (void*)(intptr_t)i) != 0) {
                LOG(ERROR) << "Fail to create bthread";
                return -1;
            }
        }
    }

    while (!brpc::IsAskedToQuit()) {
        sleep(1);
        LOG(INFO) << "Sending Request to " << FLAGS_addr 
                 << " at qps=" << g_latency_recorder.qps(1)
                 << " latency=" << g_latency_recorder.latency(1)
                 << " error=" << g_error_counter.get_value();
    }

    LOG(INFO) << "Client is going to quit";
    for (int i = 0; i < FLAGS_thread_num; ++i) {
        if (!FLAGS_use_bthread) {
            pthread_join(pids[i], NULL);
        } else {
            bthread_join(tids[i], NULL);
        }
    }
    return 0;
} 