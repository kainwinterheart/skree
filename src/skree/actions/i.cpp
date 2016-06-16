#include "i.hpp"

namespace Skree {
    namespace Actions {
        void I::in(
            const uint64_t& in_len, const char*& in_data,
            uint64_t& out_len, char*& out_data
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

            out_data = (char*)malloc(1);
            out_len += 1;

            auto eit = server.known_events.find(event_id);

            if(eit == server.known_events.end()) {
                fprintf(stderr, "[I::in] Got unknown event: %s\n", event_id);
                out_data[0] = SKREE_META_OPCODE_K; // current instance does not known
                                                   // such event, so it won't do it itself
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
                    out_data[0] = SKREE_META_OPCODE_K; // this instance has not tried
                                                       // to failover the event yet

                    auto& no_failover = event.no_failover;
                    no_failover.lock();
                    no_failover[suffix] = std::time(nullptr);
                    no_failover.unlock();

                } else {
                    out_data[0] = SKREE_META_OPCODE_F; // this instance has already
                                                       // processed the event
                }

            } else {
                auto& id = it->second;

                if(id == 0) {
                    out_data[0] = SKREE_META_OPCODE_F; // this instance is currently
                                                       // in the process of failovering
                                                       // the event

                } else {
                    auto now = std::time(nullptr);
                    auto state = server.get_event_state(id, event, now);

                    if(state == SKREE_META_EVENTSTATE_LOST) {
                        // TODO: event is processed twice here: by local node and by remote node
                        out_data[0] = SKREE_META_OPCODE_K; // this instance had lost the event

                    } else {
                        out_data[0] = SKREE_META_OPCODE_F; // event is or will be processed,
                                                           // everything is fine
                    }
                }
            }

            failover.unlock(); // TODO: transaction is too long
        }

        Utils::muh_str_t* I::out_init(
            Utils::muh_str_t*& peer_id,
            Utils::known_event_t& event,
            const uint64_t& rid_net
        ) {
            Utils::muh_str_t* out = (Utils::muh_str_t*)malloc(sizeof(*out));
            out->len = 1;
            out->data = (char*)malloc(
                out->len
                + sizeof(peer_id->len)
                + peer_id->len
                + sizeof(uint32_t) /* sizeof(event.id_len) */
                + event.id_len
                + sizeof(rid_net)
            );

            out->data[0] = opcode();

            uint32_t peer_id_len_net = htonl(peer_id->len);
            memcpy(out->data + out->len, &peer_id_len_net, sizeof(peer_id_len_net));
            out->len += sizeof(peer_id_len_net);

            memcpy(out->data + out->len, peer_id->data, peer_id->len);
            out->len += peer_id->len;

            memcpy(out->data + out->len, (char*)&(event.id_len_net), sizeof(uint32_t) /*sizeof(event.id_len)*/);
            out->len += sizeof(uint32_t) /*sizeof(event.id_len)*/;

            memcpy(out->data + out->len, event.id, event.id_len);
            out->len += event.id_len;

            memcpy(out->data + out->len, (char*)&rid_net, sizeof(rid_net));
            out->len += sizeof(rid_net);

            return out;
        }
    }
}
