#include "e.hpp"

namespace Skree {
    namespace Actions {
        void E::in(
            const uint64_t& in_len, const char*& in_data,
            Skree::Base::PendingWrite::QueueItem*& out
        ) {
            uint64_t in_pos = 0;
            uint32_t _tmp;

            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);
            uint32_t event_name_len = ntohl(_tmp);

            char event_name [event_name_len + 1];
            memcpy(event_name, in_data + in_pos, event_name_len);
            in_pos += event_name_len;
            event_name[event_name_len] = '\0';

            auto it = server.known_events.find(event_name);

            if(it == server.known_events.end()) {
                Utils::cluck(2, "[E::in] Got unknown event: %s\n", event_name);
                out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_F);
                return;
            }

            auto queue = it->second->queue;

            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);
            uint32_t cnt = ntohl(_tmp);
            const uint32_t events_count = cnt;
            in_packet_e_ctx_event* events [events_count];

            while(cnt > 0) {
                --cnt;
                in_packet_e_ctx_event* event = (in_packet_e_ctx_event*)malloc(
                    sizeof(*event));

                memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
                in_pos += sizeof(_tmp);

                event->len = ntohl(_tmp);
                event->data = (char*)(in_data + in_pos);
                event->id = nullptr;

                in_pos += event->len;

                events[cnt] = event;
            }

            // _tmp here is replication factor
            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);

            in_packet_e_ctx ctx {
                .cnt = events_count,
                .event_name_len = event_name_len,
                .event_name = event_name,
                .events = events
            };

            short result = server.save_event(&ctx, ntohl(_tmp), &client, nullptr, *queue);

            switch(result) {
                case SAVE_EVENT_RESULT_F:
                    out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_F);
                    break;
                case SAVE_EVENT_RESULT_A:
                    out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_A);
                    break;
                case SAVE_EVENT_RESULT_K:
                    out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_K);
                    break;
                case SAVE_EVENT_RESULT_NULL:
                    break;
                default:
                    Utils::cluck(2, "Unexpected save_event() result: %d\n", result);
                    abort();
            };
        }
    }
}
