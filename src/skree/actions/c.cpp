#include "c.hpp"

// debug
// #define PREV_SKREE_META_OPCODE_K SKREE_META_OPCODE_K
// #define SKREE_META_OPCODE_K SKREE_META_OPCODE_F

namespace Skree {
    namespace Actions {
        void C::in(
            const uint64_t in_len, const char* in_data,
            Skree::Base::PendingWrite::QueueItem*& out
        ) {
            // Utils::cluck(1, "CHECK");
            uint64_t in_pos = 0;

            const uint32_t event_name_len (ntohl(*(uint32_t*)(in_data + in_pos)));
            in_pos += sizeof(event_name_len);

            const char* event_name = in_data + in_pos;
            in_pos += event_name_len + 1;

            auto eit = server.known_events.find(event_name);

            if(eit == server.known_events.end()) {
                Utils::cluck(2, "[C::in] Got unknown event: %s\n", event_name);
                out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_F);
                return;
            }

            const uint64_t rid_net (ntohll(*(uint64_t*)(in_data + in_pos)));
            in_pos += sizeof(rid_net);

            const uint32_t rin_len (ntohl(*(uint32_t*)(in_data + in_pos)));
            in_pos += sizeof(rin_len);

            const char* rin (in_data + in_pos);
            in_pos += rin_len;

            uint64_t now = std::time(nullptr);
            auto state = server.get_event_state(ntohll(rid_net), *(eit->second), now);

            if(state == SKREE_META_EVENTSTATE_PROCESSED) {
                // event is processed, everything is fine
                out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_K);

            } else if(state == SKREE_META_EVENTSTATE_LOST) {
                in_packet_e_ctx_event event {
                   .len = rin_len,
                   .data = rin
                };

                in_packet_e_ctx_event* events [1];
                events[0] = &event;

                in_packet_e_ctx e_ctx {
                   .cnt = 1,
                   .event_name_len = event_name_len,
                   .event_name = event_name,
                   .events = events
                };

                short result = server.save_event(
                   &e_ctx,
                   0, // TODO: should wait for synchronous replication
                   nullptr,
                   nullptr,
                   *(eit->second->queue)
                );

                if(result == SAVE_EVENT_RESULT_K) {
                    // event has been lost, but just enqueued, again and will be re-replicated
                    out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_K);

                    auto& db = *(eit->second->queue->kv);

                    if(!db.remove((char*)&rid_net, sizeof(rid_net)))
                        Utils::cluck(2, "db.remove failed: %s\n", db.error().name());

                } else {
                    // can't re-save event, try again
                    out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_F);
                }

            } else {
                // event state is not terminal
                out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_F);
            }
        }

        Skree::Base::PendingWrite::QueueItem* C::out_init(
            Utils::known_event_t& event, const uint64_t rid_net,
            const uint32_t rin_len, const char* rin
        ) {
            const uint32_t rin_len_net = htonl(rin_len); // TODO?
            auto out = new Skree::Base::PendingWrite::QueueItem((
                sizeof(uint32_t) /*sizeof(event.id_len)*/
                + event.id_len
                + sizeof(rid_net)
                + sizeof(rin_len_net)
            ), opcode());

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
