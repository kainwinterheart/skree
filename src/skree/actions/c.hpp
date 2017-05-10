#pragma once
#include "../base/action.hpp"
#include "../meta/opcodes.hpp"
// #include "../server.hpp"

// #include <ctime>

namespace Skree {
    namespace Actions {
        class C : public Skree::Base::Action {
        public:
            static const char opcode() { return 'c'; }

            C(
                Skree::Server& _server,
                Skree::Client& _client
            ) : Skree::Base::Action(_server, _client) {}

            virtual void in(std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args) override;

            static std::shared_ptr<Skree::Base::PendingWrite::QueueItem> out_init(
                Utils::known_event_t& event, const uint64_t rid_net,
                const uint32_t rin_len, const char* rin
            );
        };
    }
}
