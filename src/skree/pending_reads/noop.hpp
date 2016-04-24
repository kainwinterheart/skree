#ifndef _SKREE_PENDINGREADS_NOOP_H_
#define _SKREE_PENDINGREADS_NOOP_H_

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

                virtual const Skree::Base::PendingRead::QueueItem* run(
                    Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item,
                    const Skree::Base::PendingRead::Callback::Args& args
                ) override { throw std::logic_error("Noop callback should not be called"); };
            };
        }

        const Skree::Base::PendingRead::QueueItem* noop(Skree::Server& server) {
            const auto cb = new Skree::PendingReads::Callbacks::Noop(server);
            const auto item = new Skree::Base::PendingRead::QueueItem {
                .len = 0,
                .cb = cb,
                .ctx = NULL,
                .opcode = false,
                .noop = true
            };

            return item;
        }
    }
}

#endif
