#ifndef _SKREE_ACTIONS_R_H_
#define _SKREE_ACTIONS_R_H_

#include "../base/action.hpp"

namespace Skree {
    struct in_packet_r_ctx;
}

#include "../server.hpp"
// #include "../meta/opcodes.hpp"

#include <list>
#include <vector>

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
            static const char opcode() { return 'r'; }

            virtual void in(
                const uint64_t& in_len, const char*& in_data,
                uint64_t& out_len, char*& out_data
            ) override;

            static Utils::muh_str_t* out_init(
                const Server& server, const uint32_t& event_name_len,
                const char*& event_name, const uint32_t& cnt
            );

            static void out_add_event(
                Utils::muh_str_t*& r_req, const uint64_t& id,
                const uint32_t& len, const char*& data
            );
        };
    }
}

#endif
