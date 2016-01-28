#include "l.hpp"

namespace Skree {
    namespace Actions {
        void L::in(
            uint64_t in_len, char* in_data,
            uint64_t* out_len, char** out_data
        ) {
            uint32_t _known_peers_len;
            char* _out_data = (char*)malloc(1 + sizeof(_known_peers_len));

            _out_data[0] = SKREE_META_OPCODE_K;
            *out_len += 1;

            pthread_mutex_lock(server->known_peers_mutex);

            _known_peers_len = htonl(server->known_peers->size());
            memcpy(_out_data + *out_len, (char*)&_known_peers_len,
                sizeof(_known_peers_len));
            *out_len += sizeof(_known_peers_len);

            for(
                known_peers_t::const_iterator it = server->known_peers->cbegin();
                it != server->known_peers->cend();
                ++it
            ) {
                Client* peer = it->second;

                uint32_t peer_name_len = peer->get_peer_name_len();
                uint32_t _peer_name_len = htonl(peer_name_len);
                uint32_t _peer_port = htonl(peer->get_peer_port());

                _out_data = (char*)realloc(_out_data, *out_len
                    + sizeof(_peer_name_len) + peer_name_len
                    + sizeof(_peer_port));

                memcpy(_out_data + *out_len, &_peer_name_len,
                        sizeof(_peer_name_len));
                *out_len += sizeof(_peer_name_len);

                memcpy(_out_data + *out_len, peer->get_peer_name(), peer_name_len);
                *out_len += peer_name_len;

                memcpy(_out_data + *out_len, &_peer_port, sizeof(_peer_port));
                *out_len += sizeof(_peer_port);
            }

            pthread_mutex_unlock(server->known_peers_mutex);

            *out_data = _out_data;
        }

        static muh_str_t* L::out_init() {
            muh_str_t* out = (muh_str_t*)malloc(sizeof(*out));
            out->len = 1;
            out->data = (char*)malloc(1);
            out->data[0] = opcode();
            return out;
        }
    }
}
