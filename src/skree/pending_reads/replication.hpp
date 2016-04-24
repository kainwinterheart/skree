#ifndef _SKREE_PENDINGREADS_REPLICATION_H_
#define _SKREE_PENDINGREADS_REPLICATION_H_

#include "../base/pending_read.hpp"
#include "../actions/r.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            class Replication : public Skree::Base::PendingRead::Callback {
            public:
                Replication(Skree::Server& _server)
                : Skree::Base::PendingRead::Callback(_server) {
                }

                virtual const Skree::Base::PendingRead::QueueItem* run(
                    Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item,
                    const Skree::Base::PendingRead::Callback::Args& args
                ) override;

                virtual void error(
                    Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item
                ) override;
            };
        }
    }
}

#endif
