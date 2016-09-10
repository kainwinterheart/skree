#pragma once
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

                virtual std::shared_ptr<Skree::Base::PendingWrite::QueueItem> run(
                    Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item,
                    std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args
                ) override;

                virtual void error(
                    Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item
                ) override;
            };
        }
    }
}
