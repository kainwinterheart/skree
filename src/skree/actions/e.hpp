#ifndef _SKREE_ACTIONS_E_H_
#define _SKREE_ACTIONS_E_H_

#include "../base/action.hpp"
#include "../meta/opcodes.hpp"

namespace Skree {
    namespace Actions {
        class E : public Skree::Base::Action {
        public:
            virtual char opcode() { return 'e'; }
        };
    }
}

#endif
