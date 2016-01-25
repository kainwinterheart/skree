#include "x.hpp"

namespace Skree {
    namespace Actions {
        void X::in(
            uint64_t in_len, char* in_data,
            uint64_t* out_len, char** out_data
        ) {
            uint64_t in_pos = 0;
            uint32_t _tmp;

            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);

            muh_str_t* peer_id = (muh_str_t*)malloc(sizeof(*peer_id));

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
            peer_id[event_id_len] = '\0';

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
            sprintf(suffix, "%s:%s:%llu", event_id, peer_id->data, rid);

            std::string rre_key("rre:", 4);
            rre_key.append(suffix, strlen(suffix));

            std::vector<std::string> keys;
            keys.push_back(rre_key);

            get_keys_result_t* dbdata = server->db->db_get_keys(keys);

            uint64_t* _rinseq = server->db->parse_db_value<uint64_t>(dbdata, &rre_key);

            if(_rinseq != NULL) {
                server->repl_clean(suffix_len, suffix, ntohll(*_rinseq));
                free(_rinseq);
            }

            delete dbdata;
        }
    }
}
