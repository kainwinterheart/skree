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
                    // TODO: crutch
                    char str [8];
                    memcpy(str, args.data, 8);
                    str[1] = str[5];
                    str[2] = str[6];
                    str[3] = str[7];

                    uint32_t protocol_version;
                    memcpy(&protocol_version, str, sizeof(protocol_version));
                    client.set_protocol_version(ntohl(protocol_version));

                    return nullptr;
                }
            };
        }
    }
}
