#ifndef _SKREE_ACTIONS_C_H_
#define _SKREE_ACTIONS_C_H_

#include "../base/action.hpp"
#include "../meta/opcodes.hpp"
#include "../server.hpp"

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

            virtual void in(
                const uint64_t& in_len, const char*& in_data,
                uint64_t& out_len, char*& out_data
            ) override;

            static Utils::muh_str_t* out_init(
                const Utils::known_event_t& event, const uint64_t& rid_net,
                const uint64_t& rin_len, char*& rin
            );
        };
    }
}

#endif
