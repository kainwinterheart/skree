#ifndef _SKREE_ACTIONS_L_H_
#define _SKREE_ACTIONS_L_H_

#include "../base/action.hpp"
#include "../meta/opcodes.hpp"
#include "../server.hpp"

// #include <pthread.h>

namespace Skree {
    namespace Actions {
        class L : public Skree::Base::Action {
        public:
            static const char opcode() { return 'l'; }

            virtual void in(
                const uint64_t& in_len, const char*& in_data,
                uint64_t& out_len, char*& out_data
            ) override;

            static Utils::muh_str_t* out_init();
        };
    }
}

#endif
