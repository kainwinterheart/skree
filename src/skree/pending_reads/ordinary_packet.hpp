#pragma once
#include "../base/pending_read.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            template<typename F>
            class OrdinaryPacket : public Skree::Base::PendingRead::Callback {
            private:
                const F cb;
            public:
                OrdinaryPacket(Skree::Server& _server, const F _cb)
                : Skree::Base::PendingRead::Callback(_server), cb(_cb) {
                }

                virtual std::shared_ptr<Skree::Base::PendingWrite::QueueItem> run(
                    Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item,
                    Skree::Base::PendingRead::Callback::Args& args
                ) override {
                    return cb(client, item, args);
                }
            };
        }
    }
}
