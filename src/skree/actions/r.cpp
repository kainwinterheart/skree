#include "r.hpp"

namespace Skree {
    namespace Actions {
        void R::in(std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args) {
            // Utils::cluck(1, "R::in begin");
            uint64_t in_pos = 0;

            uint32_t hostname_len;
            memcpy(&hostname_len, (args->data + in_pos), sizeof(hostname_len));
            in_pos += sizeof(hostname_len);
            hostname_len = ntohl(hostname_len);

            const char* hostname = args->data + in_pos; // TODO: why is this here?
            in_pos += hostname_len + 1;

            uint32_t port;
            memcpy(&port, (args->data + in_pos), sizeof(port));
            in_pos += sizeof(port);
            port = ntohl(port);

            uint32_t event_name_len;
            memcpy(&event_name_len, (args->data + in_pos), sizeof(event_name_len));
            in_pos += sizeof(event_name_len);
            event_name_len = ntohl(event_name_len);

            const char* event_name = args->data + in_pos;
            in_pos += event_name_len + 1;

            auto it = server.known_events.find(event_name);

            if(it == server.known_events.end()) {
                Utils::cluck(2, "[R::in] Got unknown event: %s\n", event_name);
                args->out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_F));
                return;
            }

            auto queue = it->second->r_queue;

            uint32_t cnt;
            memcpy(&cnt, (args->data + in_pos), sizeof(cnt));
            in_pos += sizeof(cnt);
            cnt = ntohl(cnt);

            const uint32_t events_count = cnt;
            auto events = std::make_shared<std::vector<std::shared_ptr<in_packet_r_ctx_event>>>(events_count);

            while(cnt > 0) {
                --cnt;
                uint64_t ridNet;
                memcpy(&ridNet, (args->data + in_pos), sizeof(ridNet));

                uint32_t len;
                memcpy(&len, (args->data + in_pos + sizeof(ridNet)), sizeof(len));
                len = ntohl(len);

                (*events.get())[cnt].reset(new in_packet_r_ctx_event {
                    .id_net = ridNet,
                    // .id = (char*)malloc(21),
                    .len = len,
                    .data = (args->data + in_pos + sizeof(ridNet) + sizeof(len)),
                });

                // sprintf((*events.get())[cnt]->id, "%llu", ntohll((*events.get())[cnt]->id_net)); // TODO: is this really necessary?
                // Utils::cluck(2, "repl got id: %lu\n", ntohll(events[cnt]->id_net));

                in_pos += sizeof(ridNet) + sizeof(len) + (*events.get())[cnt]->len;
            }

            memcpy(&cnt, (args->data + in_pos), sizeof(cnt));
            in_pos += sizeof(cnt);
            cnt = ntohl(cnt);

            const uint32_t peers_count = cnt;
            auto peers = std::make_shared<std::vector<std::shared_ptr<packet_r_ctx_peer>>>(peers_count);

            while(cnt > 0) {
                --cnt;

                uint32_t len;
                memcpy(&len, (args->data + in_pos), sizeof(len));
                in_pos += sizeof(len);
                len = ntohl(len);

                std::shared_ptr<Utils::muh_str_t> hostname;
                hostname.reset(new Utils::muh_str_t {
                    .own = true,
                    .len = len,
                    .data = strndup(args->data + in_pos, len)
                });

                in_pos += len + 1;

                uint32_t port;
                memcpy(&port, (args->data + in_pos), sizeof(port));
                port = ntohl(port);

                (*peers.get())[cnt].reset(new packet_r_ctx_peer {
                    .hostname = hostname,
                    .port = port,
                });

                in_pos += sizeof(port);
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

            short result = server.repl_save(ctx, client, *queue);

            if(result == REPL_SAVE_RESULT_F) {
                args->out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_F));

            } else if(result == REPL_SAVE_RESULT_K) {
                args->out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_K));

            } else {
                Utils::cluck(2, "Unexpected repl_save() result: %d\n", result);
                abort();
            }
            // Utils::cluck(1, "R::in end");
        }

        std::shared_ptr<Skree::Base::PendingWrite::QueueItem> R::out_init(
            const Server& server, const uint32_t event_name_len,
            const char* event_name, const uint32_t cnt
        ) {
            uint32_t _cnt = htonl(cnt);
            auto out = std::make_shared<Skree::Base::PendingWrite::QueueItem>(opcode());

            uint32_t _hostname_len = htonl(server.my_hostname_len);
            out->copy_concat(sizeof(_hostname_len), &_hostname_len);

            // TODO: this is unnecessary
            out->concat(server.my_hostname_len + 1, server.my_hostname);

            uint32_t _my_port = htonl(server.my_port);
            out->copy_concat(sizeof(_my_port), &_my_port);

            uint32_t _event_name_len = htonl(event_name_len);
            out->copy_concat(sizeof(_event_name_len), &_event_name_len);

            out->concat(event_name_len + 1, event_name);
            out->copy_concat(sizeof(_cnt), &_cnt);

            return out;
        }

        void R::out_add_event(
            std::shared_ptr<Skree::Base::PendingWrite::QueueItem> r_req,
            const uint64_t id, const uint32_t len, const char* data
        ) {
            uint64_t _id = htonll(id);
            r_req->copy_concat(sizeof(_id), &_id);

            uint32_t _event_len = htonl(len);
            r_req->copy_concat(sizeof(_event_len), &_event_len);

            r_req->concat(len, data);
        }
    }
}
