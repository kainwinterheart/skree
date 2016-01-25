#ifndef _SKREE_ACTIONS_R_H_
#define _SKREE_ACTIONS_R_H_

#include "../base/action.hpp"
#include "../meta/opcodes.hpp"

namespace Skree {
    namespace Actions {
        class R : public Skree::Base::Action {
        public:
            virtual char opcode() { return 'r'; }
        };
    }
}

#endif
