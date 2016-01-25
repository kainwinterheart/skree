#ifndef _SKREE_ACTIONS_C_H_
#define _SKREE_ACTIONS_C_H_

#include "../base/action.hpp"
#include "../meta/opcodes.hpp"

#include <ctime>

namespace Skree {
    namespace Actions {
        class H : public Skree::Base::Action {
        public:
            virtual char opcode() { return 'h'; }
        };
    }
}

#endif
