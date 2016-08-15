#include "e.hpp"

namespace Skree {
    namespace Actions {
        void E::in(
            const uint64_t in_len, const char* in_data,
            Skree::Base::PendingWrite::QueueItem*& out
        ) {
            uint64_t in_pos = 0;

            const uint32_t event_name_len (ntohl(*(uint32_t*)(in_data + in_pos)));
            in_pos += sizeof(event_name_len);

            const char* event_name = in_data + in_pos;
            in_pos += event_name_len + 1;

            auto it = server.known_events.find(event_name);

            if(it == server.known_events.end()) {
                Utils::cluck(2, "[E::in] Got unknown event: %s\n", event_name);
                out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_F);
                return;
            }

            auto queue = it->second->queue;

            uint32_t cnt (ntohl(*(uint32_t*)(in_data + in_pos)));
            in_pos += sizeof(cnt);

            const uint32_t events_count = cnt;
            in_packet_e_ctx_event* events [events_count];

            while(cnt > 0) {
                --cnt;

                events[cnt] = new in_packet_e_ctx_event {
                    .len = ntohl(*(uint32_t*)(in_data + in_pos)),
                    .data = (const char*)(in_data + in_pos + sizeof(uint32_t)),
                    .id = nullptr // TODO: why is it nullptr?
                };

                in_pos += sizeof(uint32_t) + events[cnt]->len;
            }

            const uint32_t replication_factor (ntohl(*(uint32_t*)(in_data + in_pos)));
            in_pos += sizeof(replication_factor);

            in_packet_e_ctx ctx {
                .cnt = events_count,
                .event_name_len = event_name_len,
                .event_name = event_name,
                .events = events
            };

            short result = server.save_event(&ctx, replication_factor, &client, nullptr, *queue);

            switch(result) {
                case SAVE_EVENT_RESULT_NULL:
                    break;
                case SAVE_EVENT_RESULT_K:
                    out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_K);
                    break;
                case SAVE_EVENT_RESULT_F:
                    out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_F);
                    break;
                case SAVE_EVENT_RESULT_A:
                    out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_A);
                    break;
                default:
                    Utils::cluck(2, "Unexpected save_event() result: %d\n", result);
                    abort();
            };
        }
    }
}
