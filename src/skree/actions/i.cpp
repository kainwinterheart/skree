#include "i.hpp"

namespace Skree {
    namespace Actions {
        void I::in(
            const uint64_t in_len, const char* in_data,
            std::shared_ptr<Skree::Base::PendingWrite::QueueItem>& out
        ) {
            uint64_t in_pos = 0;

            const uint32_t peer_id_len (ntohl(*(uint32_t*)(in_data + in_pos)));
            in_pos += sizeof(peer_id_len);

            const char* peer_id = in_data + in_pos;
            in_pos += peer_id_len + 1;

            const uint32_t event_id_len (ntohl(*(uint32_t*)(in_data + in_pos)));
            in_pos += sizeof(event_id_len);

            const char* event_id = in_data + in_pos;
            in_pos += event_id_len + 1;

            const uint64_t rid (ntohll(*(uint64_t*)(in_data + in_pos)));
            in_pos += sizeof(rid);

            auto eit = server.known_events.find(event_id);

            if(eit == server.known_events.end()) {
                Utils::cluck(2, "[I::in] Got unknown event: %s\n", event_id);
                // current instance does not known such event, so it won't do it itself
                out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_K));
                return;
            }

            char* suffix = (char*)malloc(
                peer_id_len
                + 1 // :
                + 20 // rid
                + 1 // \0
            );
            sprintf(suffix, "%s:%lu", peer_id, rid);

            auto& event = *(eit->second);
            auto& failover = event.failover;
            auto failover_end = failover.lock();
            auto it = failover.find(suffix);

            // TODO: following checks could possibly flap
            if(it == failover_end) {
                auto suffix_len = strlen(suffix);
                auto& db = *(event.r_queue->kv);
                auto size = db.check(suffix, suffix_len);

                if(size == 1) {
                    // this instance has not tried to failover the event yet
                    out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_K));

                    auto& no_failover = event.no_failover;
                    no_failover.lock();
                    no_failover[suffix] = std::time(nullptr);
                    no_failover.unlock();

                } else {
                    // this instance has already processed the event
                    out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_F));
                }

            } else {
                auto& id = it->second;

                if(id == 0) {
                    // this instance is currently in the process of failovering the event
                    out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_F));

                } else {
                    auto now = std::time(nullptr);
                    auto state = server.get_event_state(id, event, now);

                    if(state == SKREE_META_EVENTSTATE_LOST) {
                        // TODO: event is processed twice here: by local node and by remote node
                        // this instance had lost the event
                        out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_K));

                    } else {
                        // event is or will be processed, everything is fine
                        out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_F));
                    }
                }
            }

            failover.unlock(); // TODO: transaction is too long
        }

        std::shared_ptr<Skree::Base::PendingWrite::QueueItem> I::out_init(
            std::shared_ptr<Utils::muh_str_t> peer_id,
            Utils::known_event_t& event,
            const uint64_t& rid_net
        ) {
            auto out = std::make_shared<Skree::Base::PendingWrite::QueueItem>(opcode());

            uint32_t peer_id_len_net = htonl(peer_id->len);
            out->copy_concat(sizeof(peer_id_len_net), &peer_id_len_net);

            out->concat(peer_id->len + 1, peer_id->data);
            out->copy_concat(sizeof(uint32_t) /*sizeof(event.id_len)*/, &event.id_len_net);
            out->concat(event.id_len + 1, event.id);
            out->copy_concat(sizeof(rid_net), &rid_net);

            return out;
        }
    }
}
