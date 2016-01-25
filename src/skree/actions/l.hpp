#ifndef _SKREE_ACTIONS_L_H_
#define _SKREE_ACTIONS_L_H_

#include "../base/action.hpp"
#include "../meta/opcodes.hpp"

#include <pthread.h>

namespace Skree {
    namespace Actions {
        class L : public Skree::Base::Action {
        public:
            virtual char opcode() { return 'l'; }
        };
    }
}

#endif
