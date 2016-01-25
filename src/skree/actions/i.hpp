#ifndef _SKREE_ACTIONS_I_H_
#define _SKREE_ACTIONS_I_H_

#include "../base/action.hpp"
#include "../meta/opcodes.hpp"

#include <ctime>

namespace Skree {
    namespace Actions {
        class I : public Skree::Base::Action {
        public:
            virtual char opcode() { return 'i'; }
        };
    }
}

#endif
