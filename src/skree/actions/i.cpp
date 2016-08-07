#include "i.hpp"

namespace Skree {
    namespace Actions {
        void I::in(
            const uint64_t in_len, const char* in_data,
            Skree::Base::PendingWrite::QueueItem*& out
        ) {
            uint64_t in_pos = 0;
            uint32_t _tmp;

            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);
            uint32_t peer_id_len = ntohl(_tmp);

            char peer_id [peer_id_len + 1];

            memcpy(peer_id, in_data + in_pos, peer_id_len);
            in_pos += peer_id_len;
            peer_id[peer_id_len] = '\0';

            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);
            uint32_t event_id_len = ntohl(_tmp);

            char* event_id = (char*)malloc(event_id_len + 1);
            memcpy(event_id, in_data + in_pos, event_id_len);
            in_pos += event_id_len;
            event_id[event_id_len] = '\0';

            uint64_t _tmp64;
            memcpy(&_tmp64, in_data + in_pos, sizeof(_tmp64));
            in_pos += sizeof(_tmp64);
            uint64_t rid = ntohll(_tmp64);

            auto eit = server.known_events.find(event_id);

            if(eit == server.known_events.end()) {
                Utils::cluck(2, "[I::in] Got unknown event: %s\n", event_id);
                // current instance does not known such event, so it won't do it itself
                out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_K);
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
                    out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_K);

                    auto& no_failover = event.no_failover;
                    no_failover.lock();
                    no_failover[suffix] = std::time(nullptr);
                    no_failover.unlock();

                } else {
                    // this instance has already processed the event
                    out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_F);
                }

            } else {
                auto& id = it->second;

                if(id == 0) {
                    // this instance is currently in the process of failovering the event
                    out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_F);

                } else {
                    auto now = std::time(nullptr);
                    auto state = server.get_event_state(id, event, now);

                    if(state == SKREE_META_EVENTSTATE_LOST) {
                        // TODO: event is processed twice here: by local node and by remote node
                        // this instance had lost the event
                        out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_K);

                    } else {
                        // event is or will be processed, everything is fine
                        out = new Skree::Base::PendingWrite::QueueItem (0, SKREE_META_OPCODE_F);
                    }
                }
            }

            failover.unlock(); // TODO: transaction is too long
        }

        Skree::Base::PendingWrite::QueueItem* I::out_init(
            Utils::muh_str_t*& peer_id,
            Utils::known_event_t& event,
            const uint64_t& rid_net
        ) {
            auto out = new Skree::Base::PendingWrite::QueueItem((
                sizeof(peer_id->len)
                + peer_id->len
                + sizeof(uint32_t) /* sizeof(event.id_len) */
                + event.id_len
                + sizeof(rid_net)
            ), opcode());

            uint32_t peer_id_len_net = htonl(peer_id->len);
            out->push(sizeof(peer_id_len_net), &peer_id_len_net);

            out->push(peer_id->len, peer_id->data);
            out->push(sizeof(uint32_t) /*sizeof(event.id_len)*/, (char*)&(event.id_len_net));
            out->push(event.id_len, event.id);
            out->push(sizeof(rid_net), (char*)&rid_net);

            return out;
        }
    }
}
