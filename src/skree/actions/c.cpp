#include "c.hpp"

// debug
#define PREV_SKREE_META_OPCODE_K SKREE_META_OPCODE_K
#define SKREE_META_OPCODE_K SKREE_META_OPCODE_F

namespace Skree {
    namespace Actions {
        void C::in(
            const uint64_t& in_len, const char*& in_data,
            uint64_t& out_len, char*& out_data
        ) {
            uint64_t in_pos = 0;

            uint32_t event_name_len;
            memcpy(&event_name_len, in_data + in_pos, sizeof(event_name_len));
            in_pos += sizeof(event_name_len);
            event_name_len = ntohl(event_name_len);

            char event_name [event_name_len + 1];
            memcpy(event_name, in_data + in_pos, event_name_len);
            in_pos += sizeof(event_name_len);
            event_name[event_name_len] = '\0';

            auto eit = server.known_events.find(event_name);

            if(eit == server.known_events.end()) {
                fprintf(stderr, "[C::in] Got unknown event: %s\n", event_name);
                out_data = (char*)malloc(1);
                out_len += 1;
                out_data[0] = SKREE_META_OPCODE_F;
                return;
            }

            uint64_t rid;
            memcpy(&rid, in_data + in_pos, sizeof(rid));
            in_pos += sizeof(rid);
            rid = ntohll(rid);

            uint32_t rin_len;
            memcpy(&rin_len, in_data + in_pos, sizeof(rin_len));
            in_pos += sizeof(rin_len);
            rin_len = ntohl(rin_len);

            char* rin = (char*)(in_data + in_pos);
            in_pos += rin_len;

            out_data = (char*)malloc(1);
            out_len += 1;

            // size_t in_key_len = 3;
            // char* in_key = (char*)malloc(
            //     3 // in:
            //     + event_name_len
            //     + 1 // :
            //     + 20
            //     + 1 // \0
            // );
            //
            // in_key[0] = 'i';
            // in_key[1] = 'n';
            // in_key[2] = ':';
            //
            // memcpy(in_key + in_key_len, event_name, event_name_len);
            // in_key_len += event_name_len;
            //
            // in_key[in_key_len] = ':';
            // ++in_key_len;
            //
            // sprintf(in_key + in_key_len, "%llu", rid);

            bool should_save_event = false;

            wip_t::const_iterator it = server.wip.find(rid);

            if(it == server.wip.cend()) {
                // TODO: check iterator position
                // if(server.db.check(in_key, in_key_len) > 0) // TODO
                    // should_save_event = true; // TODO: is not working now

                out_data[0] = SKREE_META_OPCODE_K;

            } else {
                // TODO: check for overflow
                if((it->second + server.job_time) <= std::time(nullptr)) {
                    should_save_event = true;
                    out_data[0] = SKREE_META_OPCODE_K;
                    auto _wip = server.wip;
                    _wip.erase(it);

                } else {
                    out_data[0] = SKREE_META_OPCODE_F;
                }
            }

            if(should_save_event) {
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

                short result = server.save_event(&e_ctx, 0, NULL, NULL, *(eit->second->queue));

                if(result != SAVE_EVENT_RESULT_K) {
                    // TODO
                    // fprintf(stderr, "save_event() failed: %s\n", server.db.error().name());
                    exit(1);
                }

                // TODO
                // if(!server.db.remove(in_key, strlen(in_key)))
                //     fprintf(stderr, "db.remove failed: %s\n", server.db.error().name());
            }

            // free(in_key);
        }

        Utils::muh_str_t* C::out_init(
            const Utils::known_event_t& event, const uint64_t& rid_net,
            const uint64_t& rin_len, char*& rin
        ) {
            Utils::muh_str_t* out = (Utils::muh_str_t*)malloc(sizeof(*out));
            out->len = 1;
            out->data = (char*)malloc(
                out->len
                + event.id_len_size
                + event.id_len
                + sizeof(rid_net)
                + sizeof(rin_len)
                + rin_len
            );

            out->data[0] = opcode();

            memcpy(out->data + out->len, (char*)&(event.id_len_net), event.id_len_size);
            out->len += event.id_len_size;

            memcpy(out->data + out->len, event.id, event.id_len);
            out->len += event.id_len;

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

#define SKREE_META_OPCODE_K PREV_SKREE_META_OPCODE_K
