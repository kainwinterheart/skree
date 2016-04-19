#ifndef _SKREE_ACTIONS_H_H_
#define _SKREE_ACTIONS_H_H_

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

            virtual void in(
                const uint64_t& in_len, const char*& in_data,
                uint64_t& out_len, char*& out_data
            ) override;

            static Utils::muh_str_t* out_init(const Server& server);
        };
    }
}

#endif
