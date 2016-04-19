// #include "../base/pending_read.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            virtual const Skree::Base::PendingRead::QueueItem&& Discovery::run(
                const Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item,
                const Skree::Base::PendingRead::Callback::Args& args
            ) {
                return cb(client, item, args);
            }
        }
    }
}
