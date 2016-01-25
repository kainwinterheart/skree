#ifndef _SKREE_ACTIONS_E_H_
#define _SKREE_ACTIONS_E_H_

#include "../base/action.hpp"
#include "../meta/opcodes.hpp"

namespace Skree {
    struct in_packet_e_ctx_event {
        char* data;
        char* id;
        uint32_t len;
    };

    struct in_packet_e_ctx {
        uint32_t cnt;
        uint32_t event_name_len;
        char* event_name;
        in_packet_e_ctx_event** events;
    };

    namespace Actions {
        class E : public Skree::Base::Action {
        public:
            virtual char opcode() { return 'e'; }
        };
    }
}

#endif
