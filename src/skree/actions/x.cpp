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

            Utils::muh_str_t* peer_id = (Utils::muh_str_t*)malloc(sizeof(*peer_id));

            peer_id->len = ntohl(_tmp);
            peer_id->data = (char*)malloc(peer_id->len + 1);

            memcpy(peer_id->data, in_data + in_pos, peer_id->len);
            in_pos += peer_id->len;
            peer_id->data[peer_id->len] = '\0';

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

            size_t suffix_len =
                event_id_len
                + 1 // :
                + peer_id->len
                + 1 // :
                + 20 // rid
            ;
            char* suffix = (char*)malloc(
                suffix_len
                + 1 // \0
            );
            sprintf(suffix, "%s:%s:%lu", event_id, peer_id->data, rid);

            std::string rre_key("rre:", 4);
            rre_key.append(suffix, strlen(suffix));

            std::vector<std::string> keys;
            keys.push_back(rre_key);

            get_keys_result_t* dbdata = server.db.db_get_keys(keys);

            uint64_t* _rinseq = server.db.parse_db_value<uint64_t>(dbdata, &rre_key);

            if(_rinseq != nullptr) {
                server.repl_clean(suffix_len, suffix, ntohll(*_rinseq));
                free(_rinseq);
            }

            delete dbdata;
        }

        Utils::muh_str_t* X::out_init(
            Utils::muh_str_t*& peer_id,
            Utils::known_event_t*& event,
            const uint64_t& rid
        ) {
            Utils::muh_str_t* out = (Utils::muh_str_t*)malloc(sizeof(*out));
            out->len = 1;
            out->data = (char*)malloc(
                out->len
                + sizeof(peer_id->len)
                + peer_id->len
                + event->id_len_size
                + event->id_len
                + sizeof(rid)
            );

            out->data[0] = 'x';

            uint32_t peer_id_len_net = htonl(peer_id->len);
            memcpy(out->data + out->len, (char*)&peer_id_len_net, sizeof(peer_id_len_net));
            out->len += sizeof(peer_id_len_net);

            memcpy(out->data + out->len, peer_id->data, peer_id->len);
            out->len += peer_id->len;

            memcpy(out->data + out->len, (char*)&(event->id_len_net), event->id_len_size);
            out->len += event->id_len_size;

            memcpy(out->data + out->len, event->id, event->id_len);
            out->len += event->id_len;

            uint64_t rid_net = htonll(rid);
            memcpy(out->data + out->len, (char*)&rid_net, sizeof(rid_net));
            out->len += sizeof(rid_net);

            return out;
        }
    }
}
