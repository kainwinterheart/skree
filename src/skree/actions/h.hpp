#pragma once
#include "../base/action.hpp"
#include "../server.hpp"

// #include <pthread.h>
// #include "../meta/opcodes.hpp"

// #include <ctime>

namespace Skree {
    namespace Actions {
        class H : public Skree::Base::Action {
        public:
            static const char opcode() { return 'h'; }

            H(
                Skree::Server& _server,
                Skree::Client& _client
            ) : Skree::Base::Action(_server, _client) {}

            virtual void in(
                const uint64_t& in_len, const char*& in_data,
                Skree::Base::PendingWrite::QueueItem*& out
            ) override;

            static Skree::Base::PendingWrite::QueueItem* out_init(const Server& server);
        };
    }
}
