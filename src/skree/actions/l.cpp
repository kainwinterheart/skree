#include "l.hpp"

namespace Skree {
    namespace Actions {
        void L::in(
            const uint64_t& in_len, const char*& in_data,
            Skree::Base::PendingWrite::QueueItem*& out
        ) {
            uint32_t _known_peers_len;
            out = new Skree::Base::PendingWrite::QueueItem (sizeof(_known_peers_len), SKREE_META_OPCODE_K);

            Client* peer;
            uint32_t peer_name_len;
            uint32_t _peer_name_len;
            uint32_t _peer_port;

            auto& known_peers = server.known_peers;
            known_peers.lock();

            _known_peers_len = htonl(known_peers.size());
            out->push(sizeof(_known_peers_len), &_known_peers_len);

            for(auto& it : known_peers) {
                if(it.second.empty()) continue;
                peer = it.second[0];

                peer_name_len = peer->get_peer_name_len();
                _peer_name_len = htonl(peer_name_len);
                _peer_port = htonl(peer->get_peer_port());

                out->grow(sizeof(_peer_name_len) + peer_name_len + sizeof(_peer_port));

                out->push(sizeof(_peer_name_len), &_peer_name_len);
                out->push(peer_name_len, peer->get_peer_name());
                out->push(sizeof(_peer_port), &_peer_port);
            }

            known_peers.unlock();
        }

        Skree::Base::PendingWrite::QueueItem* L::out_init() {
            return new Skree::Base::PendingWrite::QueueItem(0, opcode());
        }
    }
}
