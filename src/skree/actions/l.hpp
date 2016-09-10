#pragma once
#include "../base/action.hpp"
#include "../meta/opcodes.hpp"
#include "../server.hpp"

// #include <pthread.h>

namespace Skree {
    namespace Actions {
        class L : public Skree::Base::Action {
        public:
            static const char opcode() { return 'l'; }

            L(
                Skree::Server& _server,
                Skree::Client& _client
            ) : Skree::Base::Action(_server, _client) {}

            virtual void in(std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args) override;

            static std::shared_ptr<Skree::Base::PendingWrite::QueueItem> out_init();
        };
    }
}
