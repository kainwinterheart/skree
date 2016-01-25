#include "r.hpp"

namespace Skree {
    namespace Actions {
        void R::in(
            uint64_t in_len, char* in_data,
            uint64_t* out_len, char** out_data
        ) {
            uint64_t in_pos = 0;
            uint32_t _tmp;

            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);
            uint32_t hostname_len = ntohl(_tmp);

            char* hostname = (char*)malloc(hostname_len);
            memcpy(hostname, in_data + in_pos, hostname_len);
            in_pos += hostname_len;

            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);
            uint32_t port = ntohl(_tmp);

            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);
            uint32_t event_name_len = ntohl(_tmp);

            char* event_name = (char*)malloc(event_name_len);
            memcpy(event_name, in_data + in_pos, event_name_len);
            in_pos += event_name_len;

            memcpy(&_tmp, in_data + in_pos, event_name_len);
            in_pos += sizeof(_tmp);
            uint32_t cnt = ntohl(_tmp);
            const uint32_t events_count = cnt;
            in_packet_r_ctx_event* events [events_count];
            uint64_t _tmp64;

            while(cnt-- > 0) {
                in_packet_r_ctx_event* event = (in_packet_r_ctx_event*)malloc(
                    sizeof(*event));

                memcpy(&_tmp64, in_data + in_pos, sizeof(_tmp64));
                in_pos += sizeof(_tmp64);
                event->id_net = _tmp64;

                event->id = (char*)malloc(21);
                sprintf(event->id, "%llu", ntohll(_tmp64));
                // printf("repl got id: %llu\n", ntohll(event->id_net));

                memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
                in_pos += sizeof(_tmp);
                event->len = ntohl(_tmp);

                event->data = (char*)malloc(event->len);
                memcpy(event->data, in_data + in_pos, event->len);
                in_pos += event->len;

                events[cnt] = event;
            }

            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);
            cnt = ntohl(_tmp);
            const uint32_t peers_count = cnt;
            packet_r_ctx_peer* peers [peers_count];

            while(cnt-- > 0) {
                packet_r_ctx_peer* peer = (packet_r_ctx_peer*)malloc(
                    sizeof(*peer));

                memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
                in_pos += sizeof(_tmp);
                peer->hostname_len = ntohl(_tmp);

                peer->hostname = (char*)malloc(peer->hostname_len + 1);
                memcpy(peer->hostname, in_data + in_pos, peer->hostname_len);
                in_pos += sizeof(peer->hostname_len);
                peer->hostname[peer->hostname_len] = '\0';

                memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
                in_pos += sizeof(_tmp);
                peer->port = ntohl(_tmp);

                peers[cnt] = peer;
            }

            in_packet_r_ctx ctx {
                .hostname_len = hostname_len,
                .port = port,
                .hostname = hostname,
                .event_name_len = event_name_len,
                .events_count = events_count,
                .peers_count = peers_count,
                .events = events,
                .peers = peers
            };

            short result = server->repl_save(&ctx, client);

            char* _out_data = (char*)malloc(1);
            *(args->out_len) += 1;
            *(args->out_data) = _out_data;

            if(result == REPL_SAVE_RESULT_F) {
                _out_data[0] = SKREE_META_OPCODE_F;

            } else if(result == REPL_SAVE_RESULT_K) {
                _out_data[0] = SKREE_META_OPCODE_K;

            } else {
                fprintf(stderr, "Unexpected repl_save() result: %d\n", result);
                exit(1);
            }
        }
    }
}
