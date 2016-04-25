#include "noop.hpp"

namespace Skree {
    namespace PendingReads {
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
