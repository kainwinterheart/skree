#include "c.hpp"
#include <ctime>

// debug
// #define PREV_SKREE_META_OPCODE_K SKREE_META_OPCODE_K
// #define SKREE_META_OPCODE_K SKREE_META_OPCODE_F

namespace Skree {
    namespace Actions {
        void C::in(std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args) {
            // Utils::cluck(1, "CHECK");
            uint64_t in_pos = 0;

            uint32_t event_name_len;
            memcpy(&event_name_len, (args->data + in_pos), sizeof(event_name_len));
            in_pos += sizeof(event_name_len);
            event_name_len = ntohl(event_name_len);

            const char* event_name = args->data + in_pos;
            in_pos += event_name_len + 1;

            auto eit = server.known_events.find(event_name);

            if(eit == server.known_events.end()) {
                Utils::cluck(2, "[C::in] Got unknown event: %s\n", event_name);
                args->out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_F));
                return;
            }

            uint64_t rid_net;
            memcpy(&rid_net, (args->data + in_pos), sizeof(rid_net));
            in_pos += sizeof(rid_net);

            const uint64_t rid = ntohll(rid_net);

            uint32_t rin_len;
            memcpy(&rin_len, (args->data + in_pos), sizeof(rin_len));
            in_pos += sizeof(rin_len);
            rin_len = ntohl(rin_len);

            const char* rin (args->data + in_pos);
            in_pos += rin_len;

            uint64_t now = std::time(nullptr);
            auto state = server.get_event_state(rid, *(eit->second), now);

            if(state == SKREE_META_EVENTSTATE_PROCESSED) {
                // event is processed, everything is fine
                args->out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_K));

            } else if(state == SKREE_META_EVENTSTATE_LOST) {
                auto events = std::make_shared<std::vector<std::shared_ptr<in_packet_e_ctx_event>>>(1);

                (*events.get())[0].reset(new in_packet_e_ctx_event {
                   .len = rin_len,
                   .data = rin
                });

                in_packet_e_ctx e_ctx {
                   .cnt = 1,
                   .event_name_len = event_name_len,
                   .event_name = event_name,
                   .events = events,
                   .origin = std::shared_ptr<void>(args, (void*)args.get())
                };

                short result = server.save_event(
                   e_ctx,
                   0, // TODO: should wait for synchronous replication
                   std::shared_ptr<Client>(),
                   nullptr,
                   *eit->second->queue->kv->NewSession(DbWrapper::TSession::ST_KV),
                   *eit->second->queue->kv->NewSession(DbWrapper::TSession::ST_QUEUE)
                );

                if(result == SAVE_EVENT_RESULT_K) {
                    // event has been lost, but just enqueued, again and will be re-replicated
                    args->out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_K));

                    auto& db = *(eit->second->queue->kv);

                    if(!db.remove((char*)&rid_net, sizeof(rid_net)))
                        Utils::cluck(1, "db.remove failed");//: %s\n", db.error().name());

                    if(!db.remove(rid))
                        Utils::cluck(1, "db.remove failed");//: %s\n", db.error().name());

                } else {
                    // can't re-save event, try again
                    args->out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_F));
                }

            } else {
                // event state is not terminal
                args->out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_F));
            }
        }

        std::shared_ptr<Skree::Base::PendingWrite::QueueItem> C::out_init(
            Utils::known_event_t& event, const uint64_t rid_net,
            const uint32_t rin_len, const char* rin
        ) {
            const uint32_t rin_len_net = htonl(rin_len); // TODO?
            auto out = std::make_shared<Skree::Base::PendingWrite::QueueItem>(opcode());

            out->copy_concat(sizeof(uint32_t) /*sizeof(event.id_len)*/, &event.id_len_net);
            out->concat(event.id_len + 1, event.id);
            out->copy_concat(sizeof(rid_net), &rid_net);
            out->copy_concat(sizeof(rin_len_net), (char*)&rin_len_net);
            out->concat(rin_len, rin);

            return out;
        }
    }
}

// #define SKREE_META_OPCODE_K PREV_SKREE_META_OPCODE_K
