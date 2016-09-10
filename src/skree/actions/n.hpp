#pragma once
#include "../base/action.hpp"
#include "../server.hpp"
#include "../meta.hpp"

namespace Skree {
    namespace Actions {
        class N : public Skree::Base::Action {
        public:
            static const char opcode() { return 'n'; }

            N(
                Skree::Server& _server,
                Skree::Client& _client
            ) : Skree::Base::Action(_server, _client) {}

            virtual void in(std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args) override;

            static std::shared_ptr<Skree::Base::PendingWrite::QueueItem> out_init();
        };
    }
}
