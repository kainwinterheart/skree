#ifndef _SKREE_SERVER_H_
#define _SKREE_SERVER_H_

#define SAVE_EVENT_RESULT_F 0
#define SAVE_EVENT_RESULT_A 1
#define SAVE_EVENT_RESULT_K 2
#define SAVE_EVENT_RESULT_NULL 3

#define REPL_SAVE_RESULT_F 0
#define REPL_SAVE_RESULT_K 1

#include "actions/c.hpp"
#include "actions/e.hpp"
#include "actions/h.hpp"
#include "actions/i.hpp"
#include "actions/l.hpp"
#include "actions/r.hpp"
#include "actions/w.hpp"
#include "actions/x.hpp"
#include "client.hpp"

namespace Skree {
    class Server {
    public:
        size_t read_size = 131072;
        uint64_t no_failover_time = 10 * 60;
        time_t discovery_timeout_milliseconds = 3000;
        uint32_t max_replication_factor = 3;
        uint32_t max_client_threads = 1;
        uint64_t job_time = 10 * 60;

        uint64_t stat_num_inserts;
        uint64_t stat_num_replications;
        pthread_mutex_t stat_mutex;

        DbWrapper db;

        char* my_hostname;
        uint32_t my_hostname_len;
        uint32_t my_port;
        char* my_peer_id;
        uint32_t my_peer_id_len;
        uint32_t my_peer_id_len_net;
        size_t my_peer_id_len_size;

        Server() {}
    }
}

#endif
