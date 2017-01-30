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
        const char* data;
        // char* id;
        uint64_t id_net;
        uint32_t len;
    };

    struct packet_r_ctx_peer {
        std::shared_ptr<Utils::muh_str_t> hostname;
        uint32_t port;
    };

    struct in_packet_r_ctx {
        uint32_t hostname_len; // TODO: probably remove this
        uint32_t port; // TODO: probably remove this
        const char* hostname; // TODO: probably remove this
        uint32_t events_count; // TODO: remove this
        uint32_t peers_count; // TODO: remove this
        std::shared_ptr<std::vector<std::shared_ptr<in_packet_r_ctx_event>>> events;
        std::shared_ptr<std::vector<std::shared_ptr<packet_r_ctx_peer>>> peers;
        const char* event_name;
        uint32_t event_name_len;
    };

    struct out_packet_r_ctx {
        uint32_t replication_factor;
        uint32_t pending;
        std::shared_ptr<Client> client;
        std::shared_ptr<std::vector<std::shared_ptr<Utils::muh_str_t>>> candidate_peer_ids;
        std::shared_ptr<std::list<std::shared_ptr<packet_r_ctx_peer>>> accepted_peers;
        std::shared_ptr<const Skree::Base::PendingWrite::QueueItem> r_req;
        std::shared_ptr<void> origin;
        bool sync;
    };

    struct out_packet_i_ctx {
        std::shared_ptr<pthread_mutex_t> mutex;
        Utils::known_event_t* event;
        std::shared_ptr<Utils::muh_str_t> data;
        std::shared_ptr<Utils::muh_str_t> peer_id;
        std::shared_ptr<Utils::muh_str_t> failover_key;
        std::shared_ptr<uint32_t> count_replicas;
        std::shared_ptr<uint32_t> pending;
        std::shared_ptr<uint32_t> acceptances;
        // char* rpr; // TODO?
        std::shared_ptr<std::deque<std::shared_ptr<Utils::muh_str_t>>> rpr;
        uint64_t rid;
        // uint32_t peers_cnt;
        std::shared_ptr<Utils::muh_str_t> origin;
    };

    namespace Actions {
        class R : public Skree::Base::Action {
        public:
            static const char opcode() { return 'r'; }

            R(
                Skree::Server& _server,
                Skree::Client& _client
            ) : Skree::Base::Action(_server, _client) {}

            virtual void in(std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args) override;

            static std::shared_ptr<Skree::Base::PendingWrite::QueueItem> out_init(
                const Server& server, const uint32_t event_name_len,
                const char* event_name, const uint32_t cnt
            );

            static void out_add_event(
                std::shared_ptr<Skree::Base::PendingWrite::QueueItem> r_req,
                const uint64_t id, const uint32_t len, const char* data
            );
        };
    }
}

#include "../server.hpp" // sorry

#endif
