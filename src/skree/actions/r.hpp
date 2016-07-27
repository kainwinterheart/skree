#ifndef _SKREE_ACTIONS_R_H_
#define _SKREE_ACTIONS_R_H_

#include "../base/action.hpp"
#include "../utils/misc.hpp"
// #include "../meta/opcodes.hpp"

#include <list>
#include <vector>
#include <pthread.h>

namespace Skree {
    struct in_packet_r_ctx_event {
        char* data;
        char* id;
        uint64_t id_net;
        uint32_t len;
    };

    struct packet_r_ctx_peer {
        uint32_t hostname_len;
        uint32_t port;
        char* hostname;
    };

    struct in_packet_r_ctx {
        uint32_t hostname_len;
        uint32_t port;
        char* hostname;
        uint32_t events_count;
        uint32_t peers_count;
        in_packet_r_ctx_event** events;
        packet_r_ctx_peer** peers;
        char* event_name;
        uint32_t event_name_len;
    };

    struct out_packet_r_ctx {
        uint32_t replication_factor;
        uint32_t pending;
        Client* client;
        std::vector<char*>* candidate_peer_ids;
        std::list<packet_r_ctx_peer*>* accepted_peers;
        const Skree::Base::PendingWrite::QueueItem* r_req;
        bool sync;
    };

    struct out_packet_i_ctx {
        pthread_mutex_t* mutex;
        Utils::known_event_t* event;
        Utils::muh_str_t* data;
        Utils::muh_str_t* peer_id;
        char* failover_key;
        uint64_t failover_key_len;
        uint32_t* count_replicas;
        uint32_t* pending;
        uint32_t* acceptances;
        char* rpr;
        uint64_t rid;
        uint32_t peers_cnt;
    };

    namespace Actions {
        class R : public Skree::Base::Action {
        public:
            static const char opcode() { return 'r'; }

            R(
                Skree::Server& _server,
                Skree::Client& _client
            ) : Skree::Base::Action(_server, _client) {}

            virtual void in(
                const uint64_t& in_len, const char*& in_data,
                Skree::Base::PendingWrite::QueueItem*& out
            ) override;

            static Skree::Base::PendingWrite::QueueItem* out_init(
                const Server& server, const uint32_t& event_name_len,
                const char*& event_name, const uint32_t& cnt
            );

            static void out_add_event(
                Skree::Base::PendingWrite::QueueItem* r_req,
                const uint64_t& id, const uint32_t& len, const char*& data
            );
        };
    }
}

#include "../server.hpp" // sorry

#endif
