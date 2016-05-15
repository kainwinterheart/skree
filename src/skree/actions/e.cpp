#include "e.hpp"

namespace Skree {
    namespace Actions {
        void E::in(
            const uint64_t& in_len, const char*& in_data,
            uint64_t& out_len, char*& out_data
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
                fprintf(stderr, "[E::in] Got unknown event: %s\n", event_name);
                out_data = (char*)malloc(1);
                out_len += 1;
                out_data[0] = SKREE_META_OPCODE_F;
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
                event->data = (char*)malloc(event->len);
                event->id = NULL;

                memcpy(event->data, in_data + in_pos, event->len);
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

            short result = server.save_event(&ctx, ntohl(_tmp), &client, NULL, *queue);

            out_data = (char*)malloc(1);
            out_len += 1;

            switch(result) {
                case SAVE_EVENT_RESULT_F:
                    out_data[0] = SKREE_META_OPCODE_F;
                    break;
                case SAVE_EVENT_RESULT_A:
                    out_data[0] = SKREE_META_OPCODE_A;
                    break;
                case SAVE_EVENT_RESULT_K:
                    out_data[0] = SKREE_META_OPCODE_K;
                    break;
                case SAVE_EVENT_RESULT_NULL:
                    free(out_data);
                    out_data = NULL;
                    out_len = 0;
                    break;
                default:
                    fprintf(stderr, "Unexpected save_event() result: %d\n", result);
                    exit(1);
            };
        }
    }
}
