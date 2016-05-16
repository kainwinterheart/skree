#ifndef _SKREE_SERVER_H_
#define _SKREE_SERVER_H_

#define SAVE_EVENT_RESULT_F 0
#define SAVE_EVENT_RESULT_A 1
#define SAVE_EVENT_RESULT_K 2
#define SAVE_EVENT_RESULT_NULL 3

#define REPL_SAVE_RESULT_F 0
#define REPL_SAVE_RESULT_K 1

// #include "actions/c.hpp"
// #include "actions/e.hpp"
// #include "actions/h.hpp"
// #include "actions/i.hpp"
// #include "actions/l.hpp"
// #include "actions/r.hpp"
// #include "actions/w.hpp"
// #include "actions/x.hpp"

namespace Skree {
    class Server;
}

#include "client.hpp"
#include "workers/client.hpp"
#include "db_wrapper.hpp"
#include "actions/e.hpp"
#include "actions/r.hpp"
#include "workers/synchronization.hpp"
#include "workers/client.hpp"
#include "workers/replication.hpp"
#include "workers/replication_exec.hpp"
#include "workers/discovery.hpp"
#include "pending_reads/replication.hpp"
#include "queue_db.hpp"

#include <stdexcept>
#include <functional>
#include <algorithm>

namespace Skree {
    struct new_client_t {
        int fh;
        std::function<void(Client&)> cb;
        sockaddr_in* s_in;
        socklen_t s_in_len;
    };

    typedef std::unordered_map<char*, uint64_t, Utils::char_pointer_hasher, Utils::char_pointer_comparator> failover_t;
    typedef std::unordered_map<uint64_t, uint64_t> wip_t;
    typedef std::unordered_map<char*, uint64_t, Utils::char_pointer_hasher, Utils::char_pointer_comparator> no_failover_t;
    typedef std::unordered_map<char*, Client*, Utils::char_pointer_hasher, Utils::char_pointer_comparator> known_peers_t;
    typedef std::unordered_map<char*, bool, Utils::char_pointer_hasher, Utils::char_pointer_comparator> me_t;

    struct peer_to_discover_t {
        const char* host;
        uint32_t port;
    };

    typedef std::unordered_map<char*, peer_to_discover_t*, Utils::char_pointer_hasher, Utils::char_pointer_comparator> peers_to_discover_t;

    class Server {
    private:
        std::queue<Workers::Client*> threads;
        pthread_t discovery;
        pthread_t replication;
        pthread_t replication_exec;
        uint32_t max_client_threads;
        void load_peers_to_discover();
        static void socket_cb(struct ev_loop* loop, ev_io* watcher, int events);
    public:
        std::queue<new_client_t*> new_clients;
        size_t read_size = 131072;
        uint64_t no_failover_time = 10 * 60;
        time_t discovery_timeout_milliseconds = 3000;
        uint32_t max_replication_factor = 3;
        uint64_t job_time = 10 * 60;

        uint64_t stat_num_inserts;
        uint64_t stat_num_replications;
        pthread_mutex_t stat_mutex;
        pthread_mutex_t new_clients_mutex;

        DbWrapper& db;

        char* my_hostname;
        uint32_t my_hostname_len;
        uint32_t my_port;
        char* my_peer_id;
        uint32_t my_peer_id_len;
        uint32_t my_peer_id_len_net;
        size_t my_peer_id_len_size;

        known_peers_t known_peers;
        known_peers_t known_peers_by_conn_id;
        pthread_mutex_t known_peers_mutex;
        no_failover_t no_failover;
        wip_t wip;
        failover_t failover;
        me_t me;
        pthread_mutex_t me_mutex;
        peers_to_discover_t peers_to_discover;
        pthread_mutex_t peers_to_discover_mutex;
        std::queue<out_packet_i_ctx*> replication_exec_queue;
        pthread_mutex_t replication_exec_queue_mutex;
        const Utils::known_events_t& known_events;

        Server(
            DbWrapper& _db, uint32_t _my_port,
            uint32_t _max_client_threads,
            const Utils::known_events_t& _known_events
        );
        virtual ~Server();

        short save_event(
            in_packet_e_ctx* ctx,
            uint32_t replication_factor,
            Client* client,
            uint64_t* task_ids,
            QueueDb& queue
        );

        short repl_save(
            in_packet_r_ctx* ctx,
            Client& client,
            QueueDb& queue
        );

        void repl_clean(
            size_t failover_key_len,
            const char* failover_key,
            uint64_t wrinseq
        );

        void unfailover(char* failover_key);
        void begin_replication(out_packet_r_ctx*& r_ctx);
        void save_peers_to_discover();
        void push_replication_exec_queue(out_packet_i_ctx* ctx);
    };
}

#endif
