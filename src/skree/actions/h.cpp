#include "h.hpp"

namespace Skree {
    namespace Actions {
        void H::in(
            const uint64_t in_len, const char* in_data,
            std::shared_ptr<Skree::Base::PendingWrite::QueueItem>& out
        ) {
            uint64_t in_pos = 0;

            const uint32_t host_len (ntohl(*(uint32_t*)(in_data + in_pos)));
            in_pos += sizeof(host_len);

            const char* host = in_data + in_pos;
            in_pos += host_len + 1;

            const uint32_t port (ntohl(*(uint32_t*)(in_data + in_pos)));
            in_pos += sizeof(port);

            // TODO: (char*)(char[len])
            char* _peer_id = Utils::make_peer_id(host_len, host, port);
            const auto conn_id = client.get_conn_id();

            auto& known_peers = server.known_peers;
            auto& known_peers_by_conn_id = server.known_peers_by_conn_id;
            auto end = known_peers.lock();
            auto known_peer = known_peers.find(_peer_id);
            known_peers_by_conn_id.lock();

            if(known_peer == end) {
                out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_K));

            } else {
                bool found = false;
                const auto& list = known_peers[_peer_id];

                for(const auto& client : list) {
                    if(strcmp(conn_id, client->get_conn_id()) == 0) {
                        found = true;
                        break;
                    }
                }

                if(found) {
                    free(_peer_id);
                    // free(host);
                    out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_F));

                } else {
                    out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_K));
                }
            }

            if(out->get_opcode() == SKREE_META_OPCODE_K) {
                client.set_peer_name(host_len, strndup(host, host_len));
                client.set_peer_port(port);
                client.set_peer_id(_peer_id);

                known_peers[_peer_id].push_back(&client);
                known_peers_by_conn_id[conn_id].push_back(&client);
            }

            known_peers_by_conn_id.unlock();
            known_peers.unlock();
        }

        std::shared_ptr<Skree::Base::PendingWrite::QueueItem> H::out_init(const Server& server) {
            auto out = std::make_shared<Skree::Base::PendingWrite::QueueItem>(opcode());

            uint32_t _hostname_len = htonl(server.my_hostname_len);
            out->copy_concat(sizeof(_hostname_len), &_hostname_len);

            out->concat(server.my_hostname_len + 1, server.my_hostname);

            uint32_t _my_port = htonl(server.my_port);
            out->copy_concat(sizeof(_my_port), &_my_port);

            return out;
        }
    }
}
