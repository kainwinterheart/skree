#pragma once
#include "../base/pending_read.hpp"
#include "../server.hpp"
#include <stdexcept>

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            class Noop : public Skree::Base::PendingRead::Callback {
            public:
                virtual bool noop() override { return true; };

                Noop(Skree::Server& _server)
                    : Skree::Base::PendingRead::Callback(_server) {};

                virtual Skree::Base::PendingWrite::QueueItem* run(
                    Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item,
                    Skree::Base::PendingRead::Callback::Args& args
                ) override { throw std::logic_error("Noop callback should not be called"); };
            };
        }

        const Skree::Base::PendingRead::QueueItem* noop(Skree::Server& server);
    }
}

