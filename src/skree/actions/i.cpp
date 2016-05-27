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
            (peer_id->data)[event_id_len] = '\0';

            uint64_t _tmp64;
            memcpy(&_tmp64, in_data + in_pos, sizeof(_tmp64));
            in_pos += sizeof(_tmp64);
            uint64_t rid = ntohll(_tmp64);

            char* _out_data = (char*)malloc(1);
            out_len += 1;
            out_data = _out_data;

            char* suffix = (char*)malloc(
                event_id_len
                + 1 // :
                + peer_id->len
                + 1 // :
                + 20 // rid
                + 1 // \0
            );
            sprintf(suffix, "%s:%s:%lu", event_id, peer_id->data, rid);

            failover_t::const_iterator it = server.failover.find(suffix);

            if(it == server.failover.cend()) {
                std::string rre_key("rre:", 4);
                rre_key.append(suffix, strlen(suffix));

                std::vector<std::string> keys;
                keys.push_back(rre_key);

                // TODO
                // get_keys_result_t* dbdata = server.db.db_get_keys(keys);

                uint64_t* _rinseq = nullptr;//server.db.parse_db_value<uint64_t>(dbdata, &rre_key);

                if(_rinseq == nullptr) {
                    _out_data[0] = SKREE_META_OPCODE_F;

                } else {
                    free(_rinseq);
                    _out_data[0] = SKREE_META_OPCODE_K;
                }

            } else {
                // TODO: It could be 0 here as a special case
                wip_t::const_iterator wip_it = server.wip.find(it->second);

                if(wip_it == server.wip.cend()) {
                    // TODO: perl
                    // if(int(rand(100) + 0.5) > 50) { // }
                    if(false) {
                        _out_data[0] = SKREE_META_OPCODE_F;

                    } else {
                        _out_data[0] = SKREE_META_OPCODE_K;

                        server.failover.erase(it);
                        server.no_failover[suffix] = std::time(nullptr);
                    }

                } else {
                    _out_data[0] = SKREE_META_OPCODE_F;
                }
            }
        }

        Utils::muh_str_t* I::out_init(
            Utils::muh_str_t*& peer_id,
            const Utils::known_event_t& event,
            const uint64_t& rid_net
        ) {
            Utils::muh_str_t* out = (Utils::muh_str_t*)malloc(sizeof(*out));
            out->len = 1;
            out->data = (char*)malloc(
                out->len
                + sizeof(peer_id->len)
                + peer_id->len
                + event.id_len_size
                + event.id_len
                + sizeof(rid_net)
            );

            out->data[0] = opcode();

            uint32_t peer_id_len_net = htonl(peer_id->len);
            memcpy(out->data + out->len, &peer_id_len_net, sizeof(peer_id_len_net));
            out->len += sizeof(peer_id_len_net);

            memcpy(out->data + out->len, peer_id->data, peer_id->len);
            out->len += peer_id->len;

            memcpy(out->data + out->len, (char*)&(event.id_len_net), event.id_len_size);
            out->len += event.id_len_size;

            memcpy(out->data + out->len, event.id, event.id_len);
            out->len += event.id_len;

            memcpy(out->data + out->len, (char*)&rid_net, sizeof(rid_net));
            out->len += sizeof(rid_net);

            return out;
        }
    }
}
