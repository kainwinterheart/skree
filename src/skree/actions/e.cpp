#include "e.hpp"
#include "../server.hpp"
#include "../client.hpp"
#include "../queue_db.hpp"

namespace Skree {
    namespace Actions {
        void E::in(std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args) {
            uint64_t in_pos = 0;

            uint32_t event_name_len;
            memcpy(&event_name_len, (args->data + in_pos), sizeof(event_name_len));
            in_pos += sizeof(event_name_len);
            event_name_len = ntohl(event_name_len);

            const char* event_name = args->data + in_pos;
            in_pos += event_name_len + 1;

            auto it = server.known_events.find(event_name);

            if(it == server.known_events.end()) {
                Utils::cluck(2, "[E::in] Got unknown event: %s\n", event_name);
                args->out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_F));
                return;
            }

            auto queue = it->second->queue;

            uint32_t cnt;
            memcpy(&cnt, args->data + in_pos, sizeof(uint32_t));
            in_pos += sizeof(cnt);
            cnt = ntohl(cnt);

            const uint32_t events_count = cnt;
            auto events = std::make_shared<std::vector<std::shared_ptr<in_packet_e_ctx_event>>>(events_count);

            while(cnt > 0) {
                --cnt;

                uint32_t len;
                memcpy(&len, args->data + in_pos, sizeof(uint32_t));

                (*events.get())[cnt].reset(new in_packet_e_ctx_event {
                    .len = ntohl(len),
                    .data = (const char*)(args->data + in_pos + sizeof(uint32_t)),
                    .id = nullptr // TODO: why is it nullptr?
                });

                in_pos += sizeof(uint32_t) + (*events.get())[cnt]->len;
            }

            uint32_t replication_factor;
            memcpy(&replication_factor, args->data + in_pos, sizeof(uint32_t));
            in_pos += sizeof(replication_factor);
            replication_factor = ntohl(replication_factor);

            in_packet_e_ctx ctx {
                .cnt = events_count,
                .event_name_len = event_name_len,
                .event_name = event_name,
                .events = events,
                .origin = std::shared_ptr<void>(args, (void*)args.get())
            };

            short result = server.save_event(
                ctx,
                replication_factor,
                client.GetNewSharedPtr(),
                nullptr,
                *queue->kv->NewSession(DbWrapper::TSession::ST_KV),
                *queue->kv->NewSession(DbWrapper::TSession::ST_QUEUE)
            );

            switch(result) {
                case SAVE_EVENT_RESULT_NULL:
                    break;
                case SAVE_EVENT_RESULT_K:
                    args->out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_K));
                    break;
                case SAVE_EVENT_RESULT_F:
                    args->out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_F));
                    break;
                case SAVE_EVENT_RESULT_A:
                    args->out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_A));
                    break;
                default:
                    Utils::cluck(2, "Unexpected save_event() result: %d\n", result);
                    abort();
            };
        }
    }
}
