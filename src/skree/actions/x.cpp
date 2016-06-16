#include "x.hpp"

namespace Skree {
    namespace Actions {
        void X::in(
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

            auto eit = server.known_events.find(event_id);

            if(eit == server.known_events.end()) {
                fprintf(stderr, "[X::in] Got unknown event: %s\n", event_id);
                return;
            }

            uint64_t _tmp64;
            memcpy(&_tmp64, in_data + in_pos, sizeof(_tmp64));
            in_pos += sizeof(_tmp64);
            uint64_t rid = ntohll(_tmp64);

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

        Utils::muh_str_t* X::out_init(
            Utils::muh_str_t*& peer_id,
            Utils::known_event_t& event,
            const uint64_t& rid
        ) {
            Utils::muh_str_t* out = (Utils::muh_str_t*)malloc(sizeof(*out));
            out->len = 1;
            out->data = (char*)malloc(
                out->len
                + sizeof(peer_id->len)
                + peer_id->len
                + sizeof(uint32_t) /*sizeof(event.id_len)*/
                + event.id_len
                + sizeof(rid)
            );

            out->data[0] = 'x';

            uint32_t peer_id_len_net = htonl(peer_id->len);
            memcpy(out->data + out->len, (char*)&peer_id_len_net, sizeof(peer_id_len_net));
            out->len += sizeof(peer_id_len_net);

            memcpy(out->data + out->len, peer_id->data, peer_id->len);
            out->len += peer_id->len;

            memcpy(out->data + out->len, (char*)&(event.id_len_net), sizeof(uint32_t) /*sizeof(event.id_len)*/);
            out->len += sizeof(uint32_t) /*sizeof(event.id_len)*/;

            memcpy(out->data + out->len, event.id, event.id_len);
            out->len += event.id_len;

            uint64_t rid_net = htonll(rid);
            memcpy(out->data + out->len, (char*)&rid_net, sizeof(rid_net));
            out->len += sizeof(rid_net);

            return out;
        }
    }
}
