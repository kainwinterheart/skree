#include <map>
#include <list>
#include <ctime>
#include <deque>
#include <queue>
#include <string>
#include <vector>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <utility>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <algorithm>
#include <pthread.h>
#include <strings.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <unordered_map>
#include <sys/types.h>

#include "tclap/CmdLine.h"
#include "yaml-cpp/yaml.h"
#include "src/skree/server.hpp"
#include "src/skree/queue_db.hpp"

#ifdef __linux__
    std::string YAML::detail::node_data::empty_scalar="";
#endif

static Skree::Utils::skree_modules_t skree_modules;
static Skree::Utils::event_groups_t event_groups;
static Skree::Utils::known_events_t known_events;

int main(int argc, char** argv) {
    std::string db_dir_name;
    std::string known_events_file_name;
    uint32_t my_port;
    uint32_t max_client_threads;

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
            1, // TODO: sane default
            "thread_count"
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

        cmd.add(_port);
        cmd.add(_max_client_threads);
        cmd.add(_db_dir_name);
        cmd.add(_known_events_file_name);

        cmd.parse(argc, argv);

        my_port = _port.getValue();
        max_client_threads = _max_client_threads.getValue();
        db_dir_name = _db_dir_name.getValue();
        known_events_file_name = _known_events_file_name.getValue();

    } catch(TCLAP::ArgException& e) {
        printf("%s %s\n", e.error().c_str(), e.argId().c_str());
    }

    YAML::Node config = YAML::LoadFile(known_events_file_name);

    {
        auto create_queue_db = [&db_dir_name](const std::string& name) {
            std::string _queue_path (db_dir_name);
            _queue_path.append("/");
            _queue_path.append(name);

            if(access(_queue_path.c_str(), R_OK) == -1) {
                mkdir(_queue_path.c_str(), 0000755);
            }

            return new Skree::QueueDb (_queue_path.c_str(), 256 * 1024 * 1024);
        };

        if(config.Type() != YAML::NodeType::Sequence) {
            fprintf(stderr, "Known events file should contain a sequence of event groups\n");
        }

        for(auto group = config.begin(); group != config.end(); ++group) {
            if(group->Type() != YAML::NodeType::Map) {
                fprintf(stderr, "Each event group should be a map\n");
                exit(1);
            }

            const YAML::Node _name = (*group)["name"];
            std::string group_name;

            if(_name && (_name.Type() == YAML::NodeType::Scalar)) {
                group_name = _name.as<std::string>();

            } else {
                fprintf(stderr, "Every event group should have a name\n");
                exit(1);
            }

            const YAML::Node _events = (*group)["events"];

            if(!_events || (_events.Type() != YAML::NodeType::Sequence)) {
                fprintf(stderr, "Every event group should have an event list\n");
                exit(1);
            }

            Skree::Utils::event_group_t* event_group = (Skree::Utils::event_group_t*)malloc(sizeof(*event_group));

            event_group->name_len = group_name.length();

            char* group_name_ = (char*)malloc(event_group->name_len + 1);
            memcpy(group_name_, group_name.c_str(), event_group->name_len);
            group_name_[event_group->name_len] = '\0';

            event_group->name = group_name_;
            // event_group->module = skree_module; // TODO

            auto it = event_groups.find(group_name_);

            if(it == event_groups.cend()) {
                event_groups[group_name_] = event_group;

            } else {
                fprintf(stderr, "Duplicate group name: %s\n", group_name_);
                exit(1);
            }

            for(auto event = _events.begin(); event != _events.end(); ++event) {
                if(event->Type() != YAML::NodeType::Map) {
                    fprintf(stderr, "Every event should be a map\n");
                    exit(1);
                }

                const YAML::Node _id = (*event)["id"];

                if(_id && (_id.Type() == YAML::NodeType::Scalar)) {
                    const YAML::Node _ttl = (*event)["ttl"];
                    uint32_t ttl;

                    if(_ttl && (_ttl.Type() == YAML::NodeType::Scalar)) {
                        ttl = _ttl.as<uint32_t>();

                    } else {
                        fprintf(stderr, "Every event should have a ttl\n");
                        exit(1);
                    }

                    std::string id = _id.as<std::string>();
                    char* id_ = strdup(id.c_str());

                    printf("id: %s, group: %s, ttl: %d\n", id_, group_name.c_str(), ttl);

                    {
                        auto it = known_events.find(id_);

                        if(it != known_events.end()) {
                            fprintf(stderr, "Duplicate event id: %s\n", id_);
                            exit(1);
                        }
                    }

                    uint32_t id_len = strlen(id_);

                    known_events[id_] = new Skree::Utils::known_event_t {
                        .id_len = id_len,
                        .id_len_net = htonl(id_len),
                        .id = id_,
                        .group = event_group,
                        .ttl = ttl,
                        .queue = create_queue_db(id),
                        .queue2 = create_queue_db(id + "/failover"),
                        .r_queue = create_queue_db(id + "/replication"),
                        .r2_queue = create_queue_db(id + "/replication_failover")
                    };

                    known_events[id_]->stat_num_processed = 0;
                    known_events[id_]->stat_num_failovered = 0;

                } else {
                    fprintf(stderr, "Every event should have an id\n");
                    exit(1);
                }
            }
        }
    }

    printf("Running on port: %u\n", my_port);
    signal(SIGPIPE, SIG_IGN);

    Skree::Server server (my_port, max_client_threads, known_events);

    return 0;
}
