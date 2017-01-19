#include "r.hpp"

namespace Skree {
    namespace Actions {
        void R::in(std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args) {
            // Utils::cluck(1, "R::in begin");
            uint64_t in_pos = 0;

            const uint32_t hostname_len (ntohl(*(uint32_t*)(args->data + in_pos)));
            in_pos += sizeof(hostname_len);

            const char* hostname = args->data + in_pos;
            in_pos += hostname_len + 1;

            const uint32_t port (ntohl(*(uint32_t*)(args->data + in_pos)));
            in_pos += sizeof(port);

            const uint32_t event_name_len (ntohl(*(uint32_t*)(args->data + in_pos)));
            in_pos += sizeof(event_name_len);

            const char* event_name = args->data + in_pos;
            in_pos += event_name_len + 1;

            auto it = server.known_events.find(event_name);

            if(it == server.known_events.end()) {
                Utils::cluck(2, "[R::in] Got unknown event: %s\n", event_name);
                args->out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_F));
                return;
            }

            auto queue = it->second->r_queue;

            uint32_t cnt (ntohl(*(uint32_t*)(args->data + in_pos)));
            in_pos += sizeof(cnt);

            const uint32_t events_count = cnt;
            auto events = std::make_shared<std::vector<std::shared_ptr<in_packet_r_ctx_event>>>(events_count);

            while(cnt > 0) {
                --cnt;

                (*events.get())[cnt].reset(new in_packet_r_ctx_event {
                    // .id_net = *(uint64_t*)(args->data + in_pos),
                    // .id = (char*)malloc(21),
                    .len = ntohl(*(uint32_t*)(args->data + in_pos + sizeof(uint64_t))),
                    .data = (args->data + in_pos + sizeof(uint64_t) + sizeof(uint32_t))
                });

                // sprintf((*events.get())[cnt]->id, "%llu", ntohll((*events.get())[cnt]->id_net)); // TODO: is this really necessary?
                // Utils::cluck(2, "repl got id: %lu\n", ntohll(events[cnt]->id_net));

                in_pos += sizeof(uint64_t) + sizeof(uint32_t) + (*events.get())[cnt]->len;
            }

            cnt = ntohl(*(uint32_t*)(args->data + in_pos));
            in_pos += sizeof(cnt);

            const uint32_t peers_count = cnt;
            auto peers = std::make_shared<std::vector<std::shared_ptr<packet_r_ctx_peer>>>(peers_count);

            while(cnt > 0) {
                --cnt;

                const uint32_t len (ntohl(*(uint32_t*)(args->data + in_pos)));
                in_pos += sizeof(len);

                std::shared_ptr<Utils::muh_str_t> hostname;
                hostname.reset(new Utils::muh_str_t {
                    .own = true,
                    .len = len,
                    .data = strndup(args->data + in_pos, len)
                });

                (*peers.get())[cnt].reset(new packet_r_ctx_peer {
                    .hostname = hostname,
                    .port = ntohl(*(uint32_t*)(args->data + in_pos + len + 1))
                });

                in_pos += len + 1 + sizeof(uint32_t);
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
