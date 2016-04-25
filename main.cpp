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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated"
#pragma clang diagnostic ignored "-Wexit-time-destructors"
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wsign-conversion"
#pragma clang diagnostic ignored "-Wpadded"
#pragma clang diagnostic ignored "-Wdocumentation-unknown-command"
#pragma clang diagnostic ignored "-Wmissing-noreturn"
#pragma clang diagnostic ignored "-Wweak-vtables"
#include "tclap/CmdLine.h"
#pragma clang diagnostic pop

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wc++98-compat"
#pragma clang diagnostic ignored "-Wsign-conversion"
#pragma clang diagnostic ignored "-Wc++98-compat-pedantic"
#pragma clang diagnostic ignored "-Wdeprecated"
#pragma clang diagnostic ignored "-Wreserved-id-macro"
#pragma clang diagnostic ignored "-Wextra-semi"
#pragma clang diagnostic ignored "-Wundef"
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"
#pragma clang diagnostic ignored "-Wpadded"
#pragma clang diagnostic ignored "-Wweak-vtables"
#include "yaml-cpp/yaml.h"
#pragma clang diagnostic pop

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wc++98-compat-pedantic"
#pragma clang diagnostic ignored "-Wc++98-compat"
#pragma clang diagnostic ignored "-Wexit-time-destructors"
#pragma clang diagnostic ignored "-Wglobal-constructors"
#pragma clang diagnostic ignored "-Wshadow"
#pragma clang diagnostic ignored "-Wold-style-cast"

// TODO

#include "src/skree/server.hpp"

// #include "src/skree/actions/c.cpp"
// #include "src/skree/actions/e.cpp"
// #include "src/skree/actions/h.cpp"
// #include "src/skree/actions/i.cpp"
// #include "src/skree/actions/l.cpp"
// #include "src/skree/actions/r.cpp"
// #include "src/skree/actions/w.cpp"
// #include "src/skree/actions/x.cpp"
// #include "src/skree/base/action.cpp"
// #include "src/skree/base/pending_read.cpp"
// #include "src/skree/base/pending_write.cpp"
// #include "src/skree/base/worker.cpp"
// #include "src/skree/client.cpp"
// #include "src/skree/db_wrapper.cpp"
// #include "src/skree/meta/opcodes.cpp"
// #include "src/skree/pending_reads/discovery.cpp"
// #include "src/skree/pending_reads/noop.cpp"
// #include "src/skree/pending_reads/ordinary_packet.cpp"
// #include "src/skree/pending_reads/replication/ping_task.cpp"
// #include "src/skree/pending_reads/replication/propose_self.cpp"
// #include "src/skree/pending_reads/replication.cpp"
// #include "src/skree/server.cpp"
// #include "src/skree/utils/misc.cpp"
// #include "src/skree/workers/client.cpp"
// #include "src/skree/workers/discovery.cpp"
// #include "src/skree/workers/replication.cpp"
// #include "src/skree/workers/replication_exec.cpp"
// #include "src/skree/workers/synchronization.cpp"

static Skree::Utils::skree_modules_t skree_modules;
static Skree::Utils::event_groups_t event_groups;
static Skree::Utils::known_events_t known_events;

int main(int argc, char** argv) {
    std::string db_file_name;
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

        TCLAP::ValueArg<std::string> _db_file_name(
            "",
            "db",
            "Database file",
            true,
            "",
            "file"
        );

        TCLAP::ValueArg<std::string> _known_events_file_name(
            "",
            "events",
            "Known events file",
            true,
            "",
            "file"
        );

        cmd.add(_port);
        cmd.add(_max_client_threads);
        cmd.add(_db_file_name);
        cmd.add(_known_events_file_name);

        cmd.parse(argc, argv);

        my_port = _port.getValue();
        max_client_threads = _max_client_threads.getValue();
        db_file_name = _db_file_name.getValue();
        known_events_file_name = _known_events_file_name.getValue();

    } catch(TCLAP::ArgException& e) {
        printf("%s %s\n", e.error().c_str(), e.argId().c_str());
    }

    YAML::Node config = YAML::LoadFile(known_events_file_name);

    {
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

                    printf("id: %s, group: %s, ttl: %d\n", id.c_str(), group_name.c_str(), ttl);

                    Skree::Utils::known_event_t* known_event =
                        (Skree::Utils::known_event_t*)malloc(sizeof(*known_event));

                    known_event->id_len = id.length();
                    known_event->id_len_net = htonl(known_event->id_len);
                    known_event->id_len_size = sizeof(known_event->id_len);

                    char* id_ = (char*)malloc(known_event->id_len + 1);
                    memcpy(id_, id.c_str(), known_event->id_len);
                    id_[known_event->id_len] = '\0';

                    known_event->id = id_;
                    known_event->group = event_group;
                    known_event->ttl = ttl;

                    auto it = known_events.find(id_);

                    if(it == known_events.cend()) {
                        known_events[id_] = known_event;

                    } else {
                        fprintf(stderr, "Duplicate event id: %s\n", id_);
                        exit(1);
                    }

                } else {
                    fprintf(stderr, "Every event should have an id\n");
                    exit(1);
                }
            }
        }
    }

    printf("Running on port: %u\n", my_port);
    signal(SIGPIPE, SIG_IGN);

    Skree::DbWrapper db;

    if(!db.open(
        db_file_name,
        kyotocabinet::HashDB::OWRITER
        | kyotocabinet::HashDB::OCREATE
        | kyotocabinet::HashDB::ONOLOCK
        | kyotocabinet::HashDB::OAUTOTRAN
    )) {
        printf("Failed to open database: %s\n", db.error().name());
        return 1;
    }

    Skree::Server server (db, my_port, max_client_threads, known_events);

    return 0;
}

#pragma clang diagnostic pop
