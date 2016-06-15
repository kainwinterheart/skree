#include "h.hpp"

namespace Skree {
    namespace Actions {
        void H::in(
            const uint64_t& in_len, const char*& in_data,
            uint64_t& out_len, char*& out_data
        ) {
            uint64_t in_pos = 0;
            uint32_t _tmp;

            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);
            uint32_t host_len = ntohl(_tmp);

            char* host = (char*)malloc(host_len + 1);
            memcpy(host, in_data + in_pos, host_len);
            in_pos += host_len;
            host[host_len] = '\0';

            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);
            uint16_t port = ntohl(_tmp);

            char* _out_data = (char*)malloc(1);
            out_data = _out_data;
            out_len = 1;

            // TODO: (char*)(char[len])
            char* _peer_id = Utils::make_peer_id(host_len, host, port);

            auto& known_peers = server.known_peers;
            auto end = known_peers.lock();
            auto known_peer = known_peers.find(_peer_id);

            if(known_peer == end) {
                out_data[0] = SKREE_META_OPCODE_K;

                client.set_peer_name(host_len, host);
                client.set_peer_port(port);
                client.set_peer_id(_peer_id);

                known_peers[_peer_id] = &client;

                auto& known_peers_by_conn_id = server.known_peers_by_conn_id;
                known_peers_by_conn_id.lock();
                known_peers_by_conn_id[client.get_conn_id()] = &client;
                known_peers_by_conn_id.unlock();

            } else {
                free(_peer_id);
                free(host);
                out_data[0] = SKREE_META_OPCODE_F;
            }

            known_peers.unlock();
        }

        Utils::muh_str_t* H::out_init(const Server& server) {
            Utils::muh_str_t* out = (Utils::muh_str_t*)malloc(sizeof(*out));
            out->len = 0;
            out->data = (char*)malloc(1
                + sizeof(server.my_hostname_len)
                + server.my_hostname_len
                + sizeof(server.my_port)
            );

            (out->data)[0] = opcode();
            out->len += 1;

            uint32_t _hostname_len = htonl(server.my_hostname_len);
            memcpy(out->data + out->len, &_hostname_len, sizeof(_hostname_len));
            out->len += sizeof(_hostname_len);

            memcpy(out->data + out->len, server.my_hostname, server.my_hostname_len);
            out->len += server.my_hostname_len;

            uint32_t _my_port = htonl(server.my_port);
            memcpy(out->data + out->len, &_my_port, sizeof(_my_port));
            out->len += sizeof(_my_port);

            return out;
        }
    }
}
