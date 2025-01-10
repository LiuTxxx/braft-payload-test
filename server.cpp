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

#include <gflags/gflags.h>              // DEFINE_*
#include <brpc/controller.h>       // brpc::Controller
#include <brpc/server.h>           // brpc::Server
#include <braft/raft.h>                  // braft::Node braft::StateMachine
#include <braft/storage.h>               // braft::SnapshotWriter
#include <braft/util.h>                  // braft::AsyncClosureGuard
#include <braft/protobuf_file.h>         // braft::ProtoBufFile
#include "payload.pb.h"                 // PayloadService

DEFINE_bool(check_term, true, "Check if the leader changed to another term");
DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
DEFINE_int32(election_timeout_ms, 5000, 
            "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(port, 8100, "Listen port of this peer");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_string(conf, "", "Initial configuration of the replication group");
DEFINE_string(data_path, "./data", "Path of data stored on");
DEFINE_string(group, "PayloadTest", "Id of the replication group");

namespace example {
class PayloadTest;

// Implements Closure which encloses RPC stuff
class ReplicatePayloadClosure : public braft::Closure {
public:
    ReplicatePayloadClosure(PayloadTest* payload_test,
                    const PayloadRequest* request,
                    PayloadResponse* response,
                    google::protobuf::Closure* done)
        : _payload_test(payload_test)
        , _request(request)
        , _response(response)
        , _done(done) {}
    ~ReplicatePayloadClosure() {}

    const PayloadRequest* request() const { return _request; }
    PayloadResponse* response() const { return _response; }
    void Run();

private:
    PayloadTest* _payload_test;
    const PayloadRequest* _request;
    PayloadResponse* _response;
    google::protobuf::Closure* _done;
};

// Implementation of example::PayloadTest as a braft::StateMachine.
class PayloadTest : public braft::StateMachine {
public:
    PayloadTest()
        : _node(NULL)
        , _leader_term(-1) {
        _current_payload.resize(256, 'x');  // Initialize with 256 bytes
    }
    ~PayloadTest() {
        delete _node;
    }

    // Starts this node
    int start() {
        butil::EndPoint addr(butil::my_ip(), FLAGS_port);
        braft::NodeOptions node_options;
        if (node_options.initial_conf.parse_from(FLAGS_conf) != 0) {
            LOG(ERROR) << "Fail to parse configuration `" << FLAGS_conf << '\'';
            return -1;
        }
        node_options.election_timeout_ms = FLAGS_election_timeout_ms;
        node_options.fsm = this;
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = FLAGS_snapshot_interval;
        std::string prefix = "local://" + FLAGS_data_path;
        node_options.log_uri = prefix + "/log";
        node_options.raft_meta_uri = prefix + "/raft_meta";
        node_options.snapshot_uri = prefix + "/snapshot";
        node_options.disable_cli = FLAGS_disable_cli;
        braft::Node* node = new braft::Node(FLAGS_group, braft::PeerId(addr));
        if (node->init(node_options) != 0) {
            LOG(ERROR) << "Fail to init raft node";
            delete node;
            return -1;
        }
        _node = node;
        return 0;
    }

    void replicate_payload(const PayloadRequest* request,
                   PayloadResponse* response,
                   google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        
        const int64_t term = _leader_term.load(butil::memory_order_relaxed);
        if (term < 0) {
            return redirect(response);
        }

        // Serialize request to IOBuf
        butil::IOBuf log;
        butil::IOBufAsZeroCopyOutputStream wrapper(&log);
        if (!request->SerializeToZeroCopyStream(&wrapper)) {
            LOG(ERROR) << "Fail to serialize request";
            response->set_success(false);
            return;
        }

        // Apply this log as a braft::Task
        braft::Task task;
        task.data = &log;
        task.done = new ReplicatePayloadClosure(this, request, response,
                                        done_guard.release());
        if (FLAGS_check_term) {
            task.expected_term = term;
        }
        
        return _node->apply(task);
    }

    void get(PayloadResponse* response) {
        // In consideration of consistency. GetRequest to follower should be 
        // rejected.
        if (!is_leader()) {
            // This node is a follower or it's not up-to-date. Redirect to
            // the leader if possible.
            return redirect(response);
        }

        // This is the leader and is up-to-date. It's safe to respond client
        response->set_success(true);
        response->set_payload(_current_payload);
    }

    bool is_leader() const { 
        return _leader_term.load(butil::memory_order_acquire) > 0; 
    }

    void shutdown() {
        if (_node) {
            _node->shutdown(NULL);
        }
    }

    void join() {
        if (_node) {
            _node->join();
        }
    }

private:
friend class ReplicatePayloadClosure;

    void redirect(PayloadResponse* response) {
        response->set_success(false);
        if (_node) {
            braft::PeerId leader = _node->leader_id();
            if (!leader.is_empty()) {
                response->set_redirect(leader.to_string());
            }
        }
    }

    void on_apply(braft::Iterator& iter) {
        for (; iter.valid(); iter.next()) {
            braft::AsyncClosureGuard closure_guard(iter.done());
            
            PayloadResponse* response = NULL;
            std::string payload;
            
            if (iter.done()) {
                ReplicatePayloadClosure* c = dynamic_cast<ReplicatePayloadClosure*>(iter.done());
                response = c->response();
                payload = c->request()->payload();
            } else {
                butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
                PayloadRequest request;
                CHECK(request.ParseFromZeroCopyStream(&wrapper));
                payload = request.payload();
            }

            // Update current payload
            _current_payload = payload;
            
            if (response) {
                response->set_success(true);
                response->set_payload(_current_payload);
            }

            LOG_IF(INFO, FLAGS_log_applied_task) 
                    << "Replicated payload of size=" << payload.size()
                    << " at log_index=" << iter.index();
        }
    }

    struct SnapshotArg {
        std::string payload;
        braft::SnapshotWriter* writer;
        braft::Closure* done;
    };

    static void *save_snapshot(void* arg) {
        SnapshotArg* sa = (SnapshotArg*) arg;
        std::unique_ptr<SnapshotArg> arg_guard(sa);
        brpc::ClosureGuard done_guard(sa->done);
        std::string snapshot_path = sa->writer->get_path() + "/data";
        LOG(INFO) << "Saving snapshot to " << snapshot_path;
        
        Snapshot s;
        s.set_payload(sa->payload);
        braft::ProtoBufFile pb_file(snapshot_path);
        if (pb_file.save(&s, true) != 0) {
            sa->done->status().set_error(EIO, "Fail to save snapshot");
            return NULL;
        }
        
        if (sa->writer->add_file("data") != 0) {
            sa->done->status().set_error(EIO, "Fail to add file to snapshot");
            return NULL;
        }
        return NULL;
    }

    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
        SnapshotArg* arg = new SnapshotArg;
        arg->payload = _current_payload;
        arg->writer = writer;
        arg->done = done;
        bthread_t tid;
        bthread_start_urgent(&tid, NULL, save_snapshot, arg);
    }

    int on_snapshot_load(braft::SnapshotReader* reader) {
        CHECK(!is_leader()) << "Leader is not supposed to load snapshot";
        if (reader->get_file_meta("data", NULL) != 0) {
            LOG(ERROR) << "Fail to find `data' on " << reader->get_path();
            return -1;
        }
        std::string snapshot_path = reader->get_path() + "/data";
        braft::ProtoBufFile pb_file(snapshot_path);
        Snapshot s;
        if (pb_file.load(&s) != 0) {
            LOG(ERROR) << "Fail to load snapshot from " << snapshot_path;
            return -1;
        }
        _current_payload = s.payload();
        return 0;
    }

    void on_leader_start(int64_t term) {
        _leader_term.store(term, butil::memory_order_release);
        LOG(INFO) << "Node becomes leader";
    }

    void on_leader_stop(const butil::Status& status) {
        _leader_term.store(-1, butil::memory_order_release);
        LOG(INFO) << "Node stepped down : " << status;
    }

    void on_shutdown() {
        LOG(INFO) << "This node is down";
    }

    void on_error(const ::braft::Error& e) {
        LOG(ERROR) << "Met raft error " << e;
    }

    void on_configuration_committed(const ::braft::Configuration& conf) {
        LOG(INFO) << "Configuration of this group is " << conf;
    }

    void on_stop_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node stops following " << ctx;
    }

    void on_start_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node start following " << ctx;
    }

private:
    braft::Node* volatile _node;
    std::string _current_payload;
    butil::atomic<int64_t> _leader_term;
};

void ReplicatePayloadClosure::Run() {
    std::unique_ptr<ReplicatePayloadClosure> self_guard(this);
    brpc::ClosureGuard done_guard(_done);
    if (status().ok()) {
        return;
    }
    _payload_test->redirect(_response);
}

// Implements example::PayloadService if you are using brpc.
class PayloadServiceImpl : public PayloadService {
public:
    explicit PayloadServiceImpl(PayloadTest* payload_test) : _payload_test(payload_test) {}
    void replicate_payload(::google::protobuf::RpcController* controller,
                   const ::example::PayloadRequest* request,
                   ::example::PayloadResponse* response,
                   ::google::protobuf::Closure* done) {
        return _payload_test->replicate_payload(request, response, done);
    }
    void get(::google::protobuf::RpcController* controller,
             const ::example::GetRequest* request,
             ::example::PayloadResponse* response,
             ::google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        return _payload_test->get(response);
    }
private:
    PayloadTest* _payload_test;
};

}  // namespace example

int main(int argc, char* argv[]) {
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    // Generally you only need one Server.
    brpc::Server server;
    example::PayloadTest payload_test;
    example::PayloadServiceImpl service(&payload_test);

    // Add service into RPC server
    if (server.AddService(&service, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    // raft can share the same RPC server
    if (braft::add_service(&server, FLAGS_port) != 0) {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
    }

    // Start the server before payload_test is started
    if (server.Start(FLAGS_port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server";
        return -1;
    }

    // Start payload_test
    if (payload_test.start() != 0) {
        LOG(ERROR) << "Fail to start payload_test";
        return -1;
    }

    LOG(INFO) << "Payload test service is running on " << server.listen_address();
    // Wait until 'CTRL-C' is pressed
    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }

    LOG(INFO) << "Payload test service is going to quit";

    // Stop payload_test before server
    payload_test.shutdown();
    server.Stop(0);

    // Wait until all the processing tasks are over
    payload_test.join();
    server.Join();
    return 0;
} 