#include "c.hpp"

namespace Skree {
    namespace Actions {
        void C::in(
            uint64_t in_len, char* in_data,
            uint64_t* out_len, char** out_data
        ) {
            uint64_t in_pos = 0;
            uint32_t _tmp;

            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);
            uint32_t event_name_len = ntohl(_tmp);

            char* event_name = (char*)malloc(event_name_len);
            memcpy(event_name, in_data + in_pos, event_name_len);
            in_pos += sizeof(event_name_len);

            uint64_t _tmp64;
            memcpy(&_tmp64, in_data + in_pos, sizeof(_tmp64));
            in_pos += sizeof(_tmp64);
            uint64_t rid = ntohll(_tmp64);

            memcpy(&_tmp, in_data + in_pos, sizeof(_tmp));
            in_pos += sizeof(_tmp);
            uint32_t rin_len = ntohl(_tmp);

            char* rin = (char*)malloc(rin_len);
            memcpy(rin, in_data + in_pos, rin_len);
            in_pos += rin_len;

            char* _out_data = (char*)malloc(1);
            *(args->out_len) += 1;
            *(args->out_data) = _out_data;

            size_t in_key_len = 3;
            char* in_key = (char*)malloc(
                3 // in:
                + event_name_len
                + 1 // :
                + 20
                + 1 // \0
            );

            in_key[0] = 'i';
            in_key[1] = 'n';
            in_key[2] = ':';

            memcpy(in_key + in_key_len, event_name, event_name_len);
            in_key_len += event_name_len;

            in_key[in_key_len] = ':';
            ++in_key_len;

            sprintf(in_key + in_key_len, "%llu", rid);

            bool should_save_event = false;

            wip_t::const_iterator it = server->wip->find(rid);

            if(it == server->wip->cend()) {
                // TODO: check iterator position
                if(server->db->check(in_key, in_key_len) > 0)
                    should_save_event = true;

                _out_data[0] = SKREE_META_OPCODE_K;

            } else {
                // TODO: check for overflow
                if((it->second + server->job_time) <= std::time(nullptr)) {
                    should_save_event = true;
                    _out_data[0] = SKREE_META_OPCODE_K;
                    server->wip->erase(it);

                } else {
                    _out_data[0] = SKREE_META_OPCODE_F;
                }
            }

            if(should_save_event) {
                in_packet_e_ctx_event* event = (in_packet_e_ctx_event*)malloc(
                    sizeof(*event));

                event->len = rin_len;
                event->data = rin;

                short result = server->save_event(e_ctx, 0, NULL, NULL);

                if(result != SAVE_EVENT_RESULT_K) {
                    fprintf(stderr, "save_event() failed: %s\n", server->db->error().name());
                    exit(1);
                }

                if(!server->db->remove(in_key, strlen(in_key)))
                    fprintf(stderr, "db.remove failed: %s\n", server->db->error().name());
            }

            free(in_key);
        }

        static muh_str_t* C::out_init(
            const known_event_t*& event, const uint64_t& rid_net,
            const uint64_t& rin_len, const char*& rin
        ) {
            muh_str_t* out = (muh_str_t*)malloc(sizeof(*out));
            out->len = 1;
            out->data = (char*)malloc(
                out->len
                + event->id_len_size
                + event->id_len
                + sizeof(rid_net)
                + sizeof(rin_len)
                + rin_len
            );

            out->data[0] = opcode();

            memcpy(out->data + out->len, (char*)&(event->id_len_net), event->id_len_size);
            out->len += event->id_len_size;

            memcpy(out->data + out->len, event->id, event->id_len);
            out->len += event->id_len;

            memcpy(out->data + out->len, (char*)&rid_net, sizeof(rid_net));
            out->len += sizeof(rid_net);

            uint32_t rin_len_net = htonl(rin_len);
            memcpy(out->data + out->len, (char*)&rin_len_net, sizeof(rin_len_net));
            out->len += sizeof(rin_len_net);

            memcpy(out->data + out->len, rin, rin_len);
            out->len += rin_len;

            return out;
        }
    }
}
