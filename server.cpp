#include <gflags/gflags.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <braft/raft.h>
#include <braft/storage.h>
#include <braft/util.h>
#include <braft/protobuf_file.h>
#include "payload.pb.h"

DEFINE_bool(check_term, true, "Check if the leader changed to another term");
DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
DEFINE_int32(election_timeout_ms, 5000, "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(port, 8100, "Listen port of this peer");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_string(conf, "", "Initial configuration of the replication group");
DEFINE_string(data_path, "./data", "Path of data stored on");
DEFINE_string(group, "PayloadTest", "Id of the replication group");
DEFINE_int32(batch_size, 400, "Batch size for raft configuration");

namespace example {
class PayloadTest;

// Closure for replicating payload
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

    // Start this node
    int start() {
        butil::EndPoint addr(butil::my_ip(), FLAGS_port);
        braft::NodeOptions node_options;
        if (node_options.initial_conf.parse_from(FLAGS_conf) != 0) {
            LOG(ERROR) << "Fail to parse configuration `" << FLAGS_conf << '\'';
            return -1;
        }
        
        // Set batch size
        node_options.election_timeout_ms = FLAGS_election_timeout_ms;
        node_options.fsm = this;
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = FLAGS_snapshot_interval;
        node_options.batch_size = FLAGS_batch_size;  // Set batch size from flags
        
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
        
        // Apply the task to the group
        return _node->apply(task);
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
            
            // Process the payload
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

    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
        // Save the current state to snapshot
        brpc::ClosureGuard done_guard(done);
        std::string snapshot_path = writer->get_path() + "/data";
        LOG(INFO) << "Saving snapshot to " << snapshot_path;
        
        Snapshot s;
        s.set_payload(_current_payload);
        
        braft::ProtoBufFile pb_file(snapshot_path);
        if (pb_file.save(&s, true) != 0) {
            done->status().set_error(EIO, "Fail to save snapshot");
            return;
        }
        
        if (writer->add_file("data") != 0) {
            done->status().set_error(EIO, "Fail to add file to snapshot");
            return;
        }
    }

    int on_snapshot_load(braft::SnapshotReader* reader) {
        std::string snapshot_path = reader->get_path() + "/data";
        LOG(INFO) << "Loading snapshot from " << snapshot_path;
        
        Snapshot s;
        braft::ProtoBufFile pb_file(snapshot_path);
        
        if (pb_file.load(&s) != 0) {
            LOG(ERROR) << "Fail to load snapshot from " << snapshot_path;
            return -1;
        }
        
        _current_payload = s.payload();
        return 0;
    }

    void on_leader_start(int64_t term) {
        _leader_term.store(term, butil::memory_order_release);
    }
    
    void on_leader_stop(const butil::Status& status) {
        _leader_term.store(-1, butil::memory_order_release);
    }

    void on_error(const ::braft::Error& e) {
        LOG(ERROR) << "Raft error: " << e;
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
    braft::Node* _node;
    std::string _current_payload;
    butil::atomic<int64_t> _leader_term;
};

void ReplicatePayloadClosure::Run() {
    // Auto delete this after Run()
    std::unique_ptr<ReplicatePayloadClosure> self_guard(this);
    brpc::ClosureGuard done_guard(_done);
}

} // namespace example

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    // Server
    brpc::Server server;
    example::PayloadTest payload_test_service;

    // Add service to server
    if (payload_test_service.start() != 0) {
        LOG(ERROR) << "Fail to start payload_test_service";
        return -1;
    }
    if (server.AddService(&payload_test_service, 
                         brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    
    // Start server
    brpc::ServerOptions options;
    options.idle_timeout_sec = -1;
    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(ERROR) << "Fail to start Server";
        return -1;
    }

    // Wait for service stop
    server.RunUntilAskedToQuit();

    // Stop service
    payload_test_service.shutdown();
    payload_test_service.join();
    return 0;
} 