#include <string>
#include <vector>
#include <utility>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <strings.h>

#include "tclap/CmdLine.h"
#include "yaml-cpp/yaml.h"
#include "src/skree/utils/misc.hpp"
#include "src/skree/utils/events.hpp"
#include "src/skree/server.hpp"
#include "src/skree/queue_db.hpp"
#include "src/skree/utils/fork_manager.hpp"
#include "src/skree/workers/fork_manager.hpp"

void signal_handler(int signal) {
    exit(0);
}

int main(int argc, char** argv) {
    signal(SIGPIPE, SIG_IGN);
    signal(SIGHUP, SIG_IGN);
    signal(SIGCHLD, SIG_IGN);
    signal(SIGTERM, signal_handler);
    signal(SIGINT, signal_handler);

    {
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setstacksize(&attr, 8 * 1024 * 1024);
        pthread_attr_destroy(&attr);
    }

    std::string db_dir_name;
    std::string known_events_file_name;
    uint32_t my_port;
    uint32_t max_client_threads;
    uint32_t max_parallel_connections;
    uint32_t max_queue_db_file_size;

    try {
        TCLAP::CmdLine cmd("skree", '=', "0.01");

        TCLAP::ValueArg<uint32_t> _port(
            "", // short param name
            "port", // long param name
            "Server port", // long description
            true, // required
            0,
            "server_port" // human-readable parameter title
        );

        TCLAP::ValueArg<uint32_t> _max_client_threads(
            "",
            "client-threads",
            "Client threads",
            false,
            60, // TODO: sane default
            "thread_count"
        );

        TCLAP::ValueArg<uint32_t> _max_parallel_connections(
            "",
            "parallel-connections",
            "Maximum parallel connections",
            false,
            20, // TODO: sane default
            "parallel_connections"
        );

        TCLAP::ValueArg<std::string> _db_dir_name(
            "",
            "db",
            "Database directory",
            true,
            "",
            "db_dir"
        );

        TCLAP::ValueArg<std::string> _known_events_file_name(
            "",
            "events",
            "Known events file",
            true,
            "",
            "events_file"
        );

        TCLAP::ValueArg<uint32_t> _max_queue_db_file_size(
            "",
            "page-size",
            "Max queue page file size (megabytes)",
            false,
            256,
            "page_size"
        );

        cmd.add(_port);
        cmd.add(_max_client_threads);
        cmd.add(_max_parallel_connections);
        cmd.add(_db_dir_name);
        cmd.add(_known_events_file_name);
        cmd.add(_max_queue_db_file_size);

        cmd.parse(argc, argv);

        my_port = _port.getValue();
        max_client_threads = _max_client_threads.getValue();
        max_parallel_connections = _max_parallel_connections.getValue();
        db_dir_name = _db_dir_name.getValue();
        known_events_file_name = _known_events_file_name.getValue();
        max_queue_db_file_size = _max_queue_db_file_size.getValue();

    } catch(TCLAP::ArgException& e) {
        printf("%s %s\n", e.error().c_str(), e.argId().c_str());
        return 1;
    }

    std::vector<Skree::Utils::TForkManager*> forkManagers;
    Skree::Utils::skree_modules_t skree_modules;
    Skree::Utils::event_groups_t event_groups;
    Skree::Utils::known_events_t known_events;

    {
        YAML::Node config = YAML::LoadFile(known_events_file_name);

        {
            auto create_queue_db = [&db_dir_name, &max_queue_db_file_size](const std::string& name) {
                std::string _queue_path (db_dir_name);
                _queue_path.append("/");
                _queue_path.append(name);

                if(access(_queue_path.c_str(), R_OK) == -1) {
                    mkdir(_queue_path.c_str(), 0000755);
                }

                return new Skree::QueueDb (
                    strdup(_queue_path.c_str()),
                    max_queue_db_file_size * 1024 * 1024
                );
            };

            if(config.Type() != YAML::NodeType::Sequence) {
                fprintf(stderr, "Known events file should contain a sequence of event groups\n");
            }

            for(const auto group : config) {
                if(group.Type() != YAML::NodeType::Map) {
                    fprintf(stderr, "Each event group should be a map\n");
                    abort();
                }

                const YAML::Node _name = group["name"];
                const YAML::Node _workersCount = group["workers_count"];
                std::string group_name;
                uint32_t workersCount = 0;

                if(_name && (_name.Type() == YAML::NodeType::Scalar)) {
                    group_name = _name.as<std::string>();

                } else {
                    fprintf(stderr, "Every event group should have a name\n");
                    abort();
                }

                if(_workersCount && (_workersCount.Type() == YAML::NodeType::Scalar)) {
                    workersCount = _workersCount.as<uint32_t>();
                }

                if(workersCount < 1) {
                    fprintf(stderr, "workers_count not specified, assuming 1\n");
                    workersCount = 3; // sane enough
                }

                const YAML::Node _events = group["events"];

                if(!_events || (_events.Type() != YAML::NodeType::Sequence)) {
                    fprintf(stderr, "Every event group should have an event list\n");
                    abort();
                }

                auto* event_group = new Skree::Utils::event_group_t();

                event_group->name_len = group_name.length();

                char* group_name_ = (char*)malloc(event_group->name_len + 1);
                memcpy(group_name_, group_name.c_str(), event_group->name_len);
                group_name_[event_group->name_len] = '\0';

                event_group->name = group_name_;

                {
                    const YAML::Node module = group["module"];

                    if(module.Type() != YAML::NodeType::Map) {
                        fprintf(stderr, "Module definition should be a map\n");
                        abort();
                    }

                    const YAML::Node _path = module["path"];
                    std::string path;

                    if(_path && (_path.Type() == YAML::NodeType::Scalar)) {
                        path = _path.as<std::string>();

                    } else {
                        fprintf(stderr, "Every module should have a path\n");
                        abort();
                    }

                    event_group->module = new Skree::Utils::skree_module_t();
                    event_group->module->path_len = path.length();
                    event_group->module->config = nullptr; // TODO

                    char* path_ = (char*)malloc(event_group->module->path_len + 1);
                    memcpy(path_, path.c_str(), event_group->module->path_len);
                    path_[event_group->module->path_len] = '\0';

                    event_group->module->path = path_;

                    Skree::Utils::cluck(2, "%s", event_group->module->path);
                    event_group->ForkManager = new Skree::Utils::TForkManager(workersCount, event_group->module);
                    forkManagers.push_back(event_group->ForkManager);
                }

                auto it = event_groups.find(group_name_);

                if(it == event_groups.cend()) {
                    event_groups[group_name_] = event_group;

                } else {
                    fprintf(stderr, "Duplicate group name: %s\n", group_name_);
                    abort();
                }

                for(const auto event : _events) {
                    if(event.Type() != YAML::NodeType::Map) {
                        fprintf(stderr, "Every event should be a map\n");
                        abort();
                    }

                    const YAML::Node _id = event["id"];

                    if(_id && (_id.Type() == YAML::NodeType::Scalar)) {
                        const YAML::Node _ttl = event["ttl"];
                        const YAML::Node _batchSize = event["batch_size"];
                        uint32_t ttl;
                        uint32_t batchSize = 1;

                        if(_ttl && (_ttl.Type() == YAML::NodeType::Scalar)) {
                            ttl = _ttl.as<uint32_t>();

                        } else {
                            fprintf(stderr, "Every event should have a ttl\n");
                            abort();
                        }

                        if(_batchSize && (_batchSize.Type() == YAML::NodeType::Scalar)) {
                            batchSize = _batchSize.as<uint32_t>();
                        }

                        if(batchSize < 1) {
                            fprintf(stderr, "batch_size not specified, assuming 1\n");
                            batchSize = 1; // sane enough
                        }

                        std::string id = _id.as<std::string>();
                        char* id_ = strdup(id.c_str());

                        printf("id: %s, group: %s, ttl: %d\n", id_, group_name.c_str(), ttl);

                        {
                            auto it = known_events.find(id_);

                            if(it != known_events.end()) {
                                fprintf(stderr, "Duplicate event id: %s\n", id_);
                                abort();
                            }
                        }

                        uint32_t id_len = strlen(id_);

                        known_events[id_] = new Skree::Utils::known_event_t {
                            .id_len = id_len,
                            .id_len_net = htonl(id_len),
                            .id = id_,
                            .group = event_group,
                            .ttl = ttl,
                            .BatchSize = batchSize,
                            .queue = create_queue_db(id),
                            .queue2 = create_queue_db(id + "/failover"),
                            .r_queue = create_queue_db(id + "/replication"),
                            .r2_queue = create_queue_db(id + "/replication_failover")
                        };

                        known_events[id_]->stat_num_processed = 0;
                        known_events[id_]->stat_num_failovered = 0;

                    } else {
                        fprintf(stderr, "Every event should have an id\n");
                        abort();
                    }
                }
            }
        }
    }

    printf("Running on port: %u\n", my_port);

    std::vector<Skree::Workers::TForkManager*> forkManagerWorkers;

    for(auto* forkManager : forkManagers) {
        auto* worker = new Skree::Workers::TForkManager(forkManager);

        worker->start();

        forkManagerWorkers.push_back(worker);
    }

    // sleep(60);
    Skree::Server server (my_port, max_client_threads, max_parallel_connections, known_events);

    for(auto* worker : forkManagerWorkers) {
        delete worker;
    }

    for(auto* forkManager : forkManagers) {
        delete forkManager;
    }

    return 0;
}
