#include "x.hpp"

namespace Skree {
    namespace Actions {
        void X::in(
            const uint64_t in_len, const char* in_data,
            Skree::Base::PendingWrite::QueueItem*& out
        ) {
            uint64_t in_pos = 0;

            const uint32_t peer_id_len (ntohl(*(uint32_t*)(in_data + in_pos)));
            in_pos += sizeof(peer_id_len);

            char peer_id [peer_id_len + 1];

            memcpy(peer_id, in_data + in_pos, peer_id_len);
            in_pos += peer_id_len;
            peer_id[peer_id_len] = '\0'; // TODO

            const uint32_t event_id_len (ntohl(*(uint32_t*)(in_data + in_pos)));
            in_pos += sizeof(event_id_len);

            char* event_id = (char*)malloc(event_id_len + 1);
            memcpy(event_id, in_data + in_pos, event_id_len);
            in_pos += event_id_len;
            event_id[event_id_len] = '\0'; // TODO

            auto eit = server.known_events.find(event_id);

            if(eit == server.known_events.end()) {
                Utils::cluck(2, "[X::in] Got unknown event: %s\n", event_id);
                return;
            }

            const uint64_t rid (ntohll(*(uint64_t*)(in_data + in_pos)));
            in_pos += sizeof(rid);

            size_t suffix_len =
                peer_id_len
                + 1 // :
                + 20 // rid
            ;
            char suffix [suffix_len + 1];
            sprintf(suffix, "%s:%lu", peer_id, rid);

            auto& event = *(eit->second);

            server.repl_clean(suffix_len, suffix, event);
            event.unfailover(suffix);
        }

        Skree::Base::PendingWrite::QueueItem* X::out_init(
            Utils::muh_str_t*& peer_id,
            Utils::known_event_t& event,
            const uint64_t& rid
        ) {
            auto out = new Skree::Base::PendingWrite::QueueItem((
                sizeof(peer_id->len)
                + peer_id->len
                + sizeof(uint32_t) /*sizeof(event.id_len)*/
                + event.id_len
                + sizeof(rid)
            ), opcode());

            uint32_t peer_id_len_net = htonl(peer_id->len);
            out->push(sizeof(peer_id_len_net), (char*)&peer_id_len_net);

            out->push(peer_id->len, peer_id->data);
            out->push(sizeof(uint32_t) /*sizeof(event.id_len)*/, (char*)&(event.id_len_net));
            out->push(event.id_len, event.id);

            uint64_t rid_net = htonll(rid);
            out->push(sizeof(rid_net), (char*)&rid_net);

            out->finish(); // TODO?

            return out;
        }
    }
}
