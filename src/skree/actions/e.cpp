#include "e.hpp"

namespace Skree {
    namespace Actions {
        void E::in(
            uint64_t in_len, char* in_data,
            uint64_t* out_len, char** out_data
        ) {
            uint64_t in_pos = 0;
            uint32_t _tmp;

            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);
            uint32_t event_name_len = ntohl(_tmp);

            char* event_name = (char*)malloc(event_name_len + 1);
            memcpy(event_name, in_data + in_pos, event_name_len);
            in_pos += event_name_len;
            event_name[event_name_len] = '\0';

            known_events_t::const_iterator it = server->known_events->find(event_name);

            if(it == server->known_events->cend()) {
                fprintf(stderr, "Got unknown event: %s\n", event_name);
                return;
            }

            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);
            uint32_t cnt = ntohl(_tmp);
            const uint32_t events_count = cnt;
            in_packet_e_ctx_event* events [events_count];

            while(cnt-- > 0) {
                in_packet_e_ctx_event* event = (in_packet_e_ctx_event*)malloc(
                    sizeof(*event));

                memcpy(&_len, in_data + in_pos, sizeof(_tmp));
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

            short result = server->save_event(&ctx, ntohl(_tmp), client, NULL);

            char* _out_data = (char*)malloc(1);
            *(args->out_len) += 1;
            *(args->out_data) = _out_data;

            switch(result) {
                case SAVE_EVENT_RESULT_F:
                    _out_data[0] = SKREE_META_OPCODE_F;
                    break;
                case SAVE_EVENT_RESULT_A:
                    _out_data[0] = SKREE_META_OPCODE_A;
                    break;
                case SAVE_EVENT_RESULT_K:
                    _out_data[0] = SKREE_META_OPCODE_K;
                    break;
                case SAVE_EVENT_RESULT_NULL:
                    free(_out_data);
                    *(args->out_data) = NULL;
                    *(args->out_len) = 0;
                    break;
                default:
                    fprintf(stderr, "Unexpected save_event() result: %d\n", result);
                    exit(1);
            };
        }
    }
}
