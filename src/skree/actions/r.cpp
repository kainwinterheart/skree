#include "r.hpp"

namespace Skree {
    namespace Actions {
        void R::in(
            const uint64_t in_len, const char* in_data,
            Skree::Base::PendingWrite::QueueItem*& out
        ) {
            // Utils::cluck(1, "R::in begin");
            uint64_t in_pos = 0;

            const uint32_t hostname_len (ntohl(*(uint32_t*)(in_data + in_pos)));
            in_pos += sizeof(hostname_len);

            char* hostname = (char*)malloc(hostname_len + 1);
            memcpy(hostname, in_data + in_pos, hostname_len);
            in_pos += hostname_len;
            hostname[hostname_len] = '\0'; // TODO

            const uint32_t port (ntohl(*(uint32_t*)(in_data + in_pos)));
            in_pos += sizeof(port);

            const uint32_t event_name_len (ntohl(*(uint32_t*)(in_data + in_pos)));
            in_pos += sizeof(event_name_len);

            char event_name [event_name_len + 1];
            memcpy(event_name, in_data + in_pos, event_name_len);
            in_pos += event_name_len;
            event_name[event_name_len] = '\0'; // TODO

            auto it = server.known_events.find(event_name);

            if(it == server.known_events.end()) {
                Utils::cluck(2, "[R::in] Got unknown event: %s\n", event_name);
                out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_F);
                return;
            }

            auto queue = it->second->r_queue;

            uint32_t cnt (ntohl(*(uint32_t*)(in_data + in_pos)));
            in_pos += sizeof(cnt);

            const uint32_t events_count = cnt;
            in_packet_r_ctx_event* events [events_count];

            while(cnt > 0) {
                --cnt;

                events[cnt] = new in_packet_r_ctx_event {
                    .id_net = *(uint64_t*)(in_data + in_pos),
                    .id = (char*)malloc(21),
                    .len = ntohl(*(uint32_t*)(in_data + in_pos + sizeof(uint64_t))),
                    .data = (in_data + in_pos + sizeof(uint64_t) + sizeof(uint32_t))
                };

                sprintf(events[cnt]->id, "%llu", ntohll(events[cnt]->id_net)); // TODO: is this really necessary?
                // Utils::cluck(2, "repl got id: %lu\n", ntohll(events[cnt]->id_net));

                in_pos += sizeof(uint64_t) + sizeof(uint32_t) + events[cnt]->len;
            }

            cnt = ntohl(*(uint32_t*)(in_data + in_pos));
            in_pos += sizeof(cnt);

            const uint32_t peers_count = cnt;
            packet_r_ctx_peer* peers [peers_count];

            while(cnt > 0) {
                --cnt;

                const uint32_t len (ntohl(*(uint32_t*)(in_data + in_pos)));
                in_pos += sizeof(len);

                peers[cnt] = new packet_r_ctx_peer {
                    .hostname_len = len,
                    .hostname = (char*)malloc(len + 1),
                    .port = ntohl(*(uint32_t*)(in_data + in_pos + len))
                };

                memcpy(peers[cnt]->hostname, in_data + in_pos, len);
                peers[cnt]->hostname[len] = '\0'; // TODO

                in_pos += len + sizeof(uint32_t);
            }

            in_packet_r_ctx ctx {
                .hostname_len = hostname_len,
                .port = port,
                .hostname = hostname,
                .event_name_len = event_name_len,
                .event_name = event_name,
                .events_count = events_count,
                .peers_count = peers_count,
                .events = events,
                .peers = peers
            };

            short result = server.repl_save(&ctx, client, *queue);

            if(result == REPL_SAVE_RESULT_F) {
                out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_F);

            } else if(result == REPL_SAVE_RESULT_K) {
                out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_K);

            } else {
                Utils::cluck(2, "Unexpected repl_save() result: %d\n", result);
                abort();
            }
            // Utils::cluck(1, "R::in end");
        }

        Skree::Base::PendingWrite::QueueItem* R::out_init(
            const Server& server, const uint32_t& event_name_len,
            const char*& event_name, const uint32_t& cnt
        ) {
            uint32_t _cnt = htonl(cnt);
            auto out = new Skree::Base::PendingWrite::QueueItem((
                sizeof(server.my_hostname_len)
                + server.my_hostname_len
                + sizeof(server.my_port)
                + sizeof(event_name_len)
                + event_name_len
                + sizeof(_cnt)
            ), opcode());

            uint32_t _hostname_len = htonl(server.my_hostname_len);
            out->push(sizeof(_hostname_len), &_hostname_len);

            out->push(server.my_hostname_len, server.my_hostname);

            uint32_t _my_port = htonl(server.my_port);
            out->push(sizeof(_my_port), (char*)&_my_port);

            uint32_t _event_name_len = htonl(event_name_len);
            out->push(sizeof(_event_name_len), (char*)&_event_name_len);

            out->push(event_name_len, event_name);
            out->push(sizeof(_cnt), (char*)&_cnt);

            return out;
        }

        void R::out_add_event(
            Skree::Base::PendingWrite::QueueItem* r_req,
            const uint64_t& id, const uint32_t& len, const char*& data
        ) {
            uint64_t _id = htonll(id);
            r_req->grow(sizeof(_id) + sizeof(len) + len);

            r_req->push(sizeof(_id), (char*)&_id);

            uint32_t _event_len = htonl(len);
            r_req->push(sizeof(_event_len), (char*)&_event_len);

            r_req->push(len, data);
        }
    }
}
