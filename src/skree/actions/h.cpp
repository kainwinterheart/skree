#include "h.hpp"

namespace Skree {
    namespace Actions {
        void H::in(
            uint64_t in_len, char* in_data,
            uint64_t* out_len, char** out_data
        ) {
            uint64_t in_pos = 0;
            uint32_t _tmp;

            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);
            uint32_t host_len = ntohl(_tmp);

            char host[host_len];
            memcpy(host, in_data + in_pos, host_len);
            in_pos += host_len;

            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);
            uint32_t port = ntohl(_tmp);

            char* out_data = (char*)malloc(1);
            *(args->out_data) = out_data;
            *(args->out_len) = 1;

            char* _peer_id = server->make_peer_id(host_len, host, port);

            pthread_mutex_lock(server->known_peers_mutex);

            known_peers_t::const_iterator known_peer = server->known_peers->find(_peer_id);

            if(known_peer == server->known_peers->cend()) {
                out_data[0] = SKREE_META_OPCODE_K;

                peer_name = (char*)malloc(host_len);
                memcpy(peer_name, host, host_len);

                peer_name_len = host_len;
                peer_port = port;
                peer_id = _peer_id;

                server->known_peers[_peer_id] = client;
                server->known_peers_by_conn_id[client->get_conn_id()] = client;

            } else {
                free(_peer_id);
                out_data[0] = SKREE_META_OPCODE_F;
            }

            pthread_mutex_unlock(server->known_peers_mutex);
        }

        static muh_str_t* H::out_init(const Server*& server) {
            muh_str_t* out = (muh_str_t*)malloc(sizeof(*out));
            out->len = 0;
            out->data = (char*)malloc(1
                + sizeof(server->my_hostname_len)
                + server->my_hostname_len
                + sizeof(server->my_port)
            );

            h_req[0] = opcode();
            h_len += 1;

            uint32_t _hostname_len = htonl(server->my_hostname_len);
            memcpy(h_req + h_len, &_hostname_len, sizeof(_hostname_len));
            h_len += sizeof(_hostname_len);

            memcpy(h_req + h_len, server->my_hostname, server->my_hostname_len);
            h_len += server->my_hostname_len;

            uint32_t _my_port = htonl(server->my_port);
            memcpy(h_req + h_len, &_my_port, sizeof(_my_port));
            h_len += sizeof(_my_port);

            return out;
        }
    }
}
