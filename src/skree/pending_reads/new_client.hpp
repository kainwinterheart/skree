#pragma once
#include "../base/pending_read.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            class NewClient : public Skree::Base::PendingRead::Callback {
            public:
                NewClient(Skree::Server& _server)
                : Skree::Base::PendingRead::Callback(_server) {
                }

                virtual Skree::Base::PendingWrite::QueueItem* run(
                    Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item,
                    Skree::Base::PendingRead::Callback::Args& args
                ) override {
                    uint32_t protocol_version;
                    memcpy(&protocol_version, args.data + 4, sizeof(protocol_version));
                    client.set_protocol_version(ntohl(protocol_version));

                    return nullptr;
                }
            };
        }
    }
}
