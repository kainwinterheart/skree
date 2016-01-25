#ifndef _SKREE_ACTIONS_X_H_
#define _SKREE_ACTIONS_X_H_

#include "../base/action.hpp"
#include "../meta/opcodes.hpp"

#include <ctime>

namespace Skree {
    namespace Actions {
        class X : public Skree::Base::Action {
        public:
            virtual char opcode() { return 'x'; }
        };
    }
}

#endif
