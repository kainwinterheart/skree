#pragma once
#include "../base/action.hpp"
#include "../utils/misc.hpp"
#include "../server.hpp"
// #include "../meta/opcodes.hpp"

// #include <ctime>

namespace Skree {
    namespace Actions {
        class I : public Skree::Base::Action {
        public:
            static const char opcode() { return 'i'; }

            I(
                Skree::Server& _server,
                Skree::Client& _client
            ) : Skree::Base::Action(_server, _client) {}

            virtual void in(
                const uint64_t& in_len, const char*& in_data,
                uint64_t& out_len, char*& out_data
            ) override;

            static Utils::muh_str_t* out_init(
                Utils::muh_str_t*& peer_id,
                Utils::known_event_t& event,
                const uint64_t& rid_net
            );
        };
    }
}

