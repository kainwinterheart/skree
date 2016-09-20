#include "x.hpp"

namespace Skree {
    namespace Actions {
        void X::in(std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args) {
            uint64_t in_pos = 0;

            const uint32_t peer_id_len (ntohl(*(uint32_t*)(args->data + in_pos)));
            in_pos += sizeof(peer_id_len);

            const char* peer_id = args->data + in_pos;
            in_pos += peer_id_len + 1;

            const uint32_t event_id_len (ntohl(*(uint32_t*)(args->data + in_pos)));
            in_pos += sizeof(event_id_len);

            const char* event_id = args->data + in_pos;
            in_pos += event_id_len + 1;

            auto eit = server.known_events.find(event_id);

            if(eit == server.known_events.end()) {
                Utils::cluck(2, "[X::in] Got unknown event: %s\n", event_id);
                return;
            }

            const uint64_t rid (ntohll(*(uint64_t*)(args->data + in_pos)));
            in_pos += sizeof(rid);

            auto suffix = Utils::NewStr(
                peer_id_len
                + 1 // :
                + 20 // rid
                + 1 // \0
            );

            sprintf(suffix->data, "%s:%lu", peer_id, rid);
            suffix->len = strlen(suffix->data);

            auto& event = *(eit->second);

            server.repl_clean(suffix->len, suffix->data, event);
            event.unfailover(suffix);
        }

        std::shared_ptr<Skree::Base::PendingWrite::QueueItem> X::out_init(
            std::shared_ptr<Utils::muh_str_t> peer_id,
            Utils::known_event_t& event,
            const uint64_t& rid
        ) {
            auto out = std::make_shared<Skree::Base::PendingWrite::QueueItem>(opcode());

            uint32_t peer_id_len_net = htonl(peer_id->len);
            out->copy_concat(sizeof(peer_id_len_net), &peer_id_len_net);

            out->concat(peer_id->len + 1, peer_id->data);
            out->copy_concat(sizeof(uint32_t) /*sizeof(event.id_len)*/, &event.id_len_net);
            out->concat(event.id_len + 1, event.id);

            uint64_t rid_net = htonll(rid);
            out->copy_concat(sizeof(rid_net), &rid_net);

            out->finish(); // TODO?

            return out;
        }
    }
}
