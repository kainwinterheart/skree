#ifndef _SKREE_ACTIONS_R_H_
#define _SKREE_ACTIONS_R_H_

#include "../base/action.hpp"
#include "../meta/opcodes.hpp"

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
        char* r_req;
        size_t r_len;
        bool sync;
    };

    namespace Actions {
        class R : public Skree::Base::Action {
        public:
            virtual char opcode() { return 'r'; }
        };
    }
}

#endif
