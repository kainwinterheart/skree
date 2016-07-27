#pragma once
#include "../base/action.hpp"
#include "../server.hpp"
// #include "../meta/opcodes.hpp"

// #include <ctime>

namespace Skree {
    namespace Actions {
        class X : public Skree::Base::Action {
        public:
            static const char opcode() { return 'x'; }

            X(
                Skree::Server& _server,
                Skree::Client& _client
            ) : Skree::Base::Action(_server, _client) {}

            virtual void in(
                const uint64_t& in_len, const char*& in_data,
                Skree::Base::PendingWrite::QueueItem*& out
            ) override;

            static Skree::Base::PendingWrite::QueueItem* out_init(
                Utils::muh_str_t*& peer_id,
                Utils::known_event_t& event,
                const uint64_t& rid
            );
        };
    }
}
