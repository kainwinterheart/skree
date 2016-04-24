#ifndef _SKREE_PENDINGREADS_ORDINARYPACKET_C_
#define _SKREE_PENDINGREADS_ORDINARYPACKET_C_

#include "ordinary_packet.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            template<typename F>
            const Skree::Base::PendingRead::QueueItem* OrdinaryPacket<F>::run(
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
