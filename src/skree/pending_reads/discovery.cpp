#ifndef _SKREE_PENDINGREADS_DISCOVERY_C_
#define _SKREE_PENDINGREADS_DISCOVERY_C_

#include "discovery.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            template<typename F>
            const Skree::Base::PendingRead::QueueItem* Discovery<F>::run(
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item,
                const Skree::Base::PendingRead::Callback::Args& args
            ) {
                return cb(client, item, args);
            }
        }
    }
}

#endif
