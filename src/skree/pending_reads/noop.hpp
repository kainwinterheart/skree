#ifndef _SKREE_PENDINGREADS_NOOP_H_
#define _SKREE_PENDINGREADS_NOOP_H_

// #include "../base/pending_read.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            class Noop : public Skree::Base::PendingRead::Callback {
                virtual bool noop() { return true; };
            };
        }

        const Skree::Base::PendingRead::QueueItem&& noop(const Skree::Server& server) {
            const Skree::PendingReads::Callbacks::Noop cb (server);
            const Skree::Base::PendingRead::QueueItem item {
                .len = 0,
                .cb = std::move(cb),
                .ctx = NULL,
                .opcode = false,
                .noop = true
            };

            return std::move(item);
        }
    }
}

#endif
