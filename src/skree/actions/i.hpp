#ifndef _SKREE_ACTIONS_I_H_
#define _SKREE_ACTIONS_I_H_

#include "../base/action.hpp"
#include "../utils/misc.hpp"
#include "../server.hpp"
// #include "../meta/opcodes.hpp"

// #include <ctime>

namespace Skree {
    namespace Actions {
        class I : public Skree::Base::Action {
        public:
            static const char opcode() { return 'i'; }

            virtual void in(
                const uint64_t& in_len, const char*& in_data,
                uint64_t& out_len, char*& out_data
            ) override;

            static Utils::muh_str_t* out_init(
                const Utils::muh_str_t*& peer_id,
                const Utils::known_event_t*& event,
                const uint64_t& rid_net
            );
        };
    }
}

#endif
