#include "l.hpp"

namespace Skree {
    namespace Actions {
        void L::in(std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args) {
            uint32_t _known_peers_len;
            args->out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_K));

            auto& known_peers = server.known_peers;
            known_peers.lock();

            _known_peers_len = htonl(known_peers.size());
            args->out->copy_concat(sizeof(_known_peers_len), &_known_peers_len);

            for(auto& it : known_peers) {
                if(it.second.empty()) continue;
                auto peer = it.second[0];

                const auto& peer_name = peer->get_peer_name();
                uint32_t _peer_name_len = htonl(peer_name->len);
                uint32_t _peer_port = htonl(peer->get_peer_port());

                // out->grow(sizeof(_peer_name_len) + peer_name_len + sizeof(_peer_port));

                args->out->copy_concat(sizeof(_peer_name_len), &_peer_name_len);
                args->out->concat(peer_name->len + 1, peer_name->data);
                args->out->copy_concat(sizeof(_peer_port), &_peer_port);
            }

            known_peers.unlock();
        }

        std::shared_ptr<Skree::Base::PendingWrite::QueueItem> L::out_init() {
            return std::make_shared<Skree::Base::PendingWrite::QueueItem>(opcode());
        }
    }
}
