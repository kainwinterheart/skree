#ifndef _SKREE_ACTIONS_W_H_
#define _SKREE_ACTIONS_W_H_

#include "../base/action.hpp"
#include "../meta/opcodes.hpp"

namespace Skree {
    namespace Actions {
        class W : public Skree::Base::Action {
        public:
            virtual char opcode() { return 'w'; }
        };
    }
}

#endif
