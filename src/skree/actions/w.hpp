#ifndef _SKREE_ACTIONS_W_H_
#define _SKREE_ACTIONS_W_H_

#include "../base/action.hpp"
#include "../server.hpp"
// #include "../meta/opcodes.hpp"

namespace Skree {
    namespace Actions {
        class W : public Skree::Base::Action {
        public:
            static const char opcode() { return 'w'; }

            W(
                Skree::Server& _server,
                Skree::Client& _client
            ) : Skree::Base::Action(_server, _client) {}

            virtual void in(
                const uint64_t& in_len, const char*& in_data,
                uint64_t& out_len, char*& out_data
            ) override;

            static Utils::muh_str_t* out_init();
        };
    }
}

#endif
