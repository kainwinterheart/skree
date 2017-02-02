#include "r.hpp"

namespace Skree {
    namespace Actions {
        void R::in(std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args) {
            // Utils::cluck(1, "R::in begin");
            uint64_t in_pos = 0;

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

            const auto& peer_name = client.get_peer_name();

            in_packet_r_ctx ctx {
                .hostname_len = peer_name->len,
                .port = client.get_peer_port(),
                .hostname = peer_name->data,
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
    }
}
