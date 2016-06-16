#pragma once
#include "../../base/pending_read.hpp"
#include "../../utils/misc.hpp"
#include "../../meta/opcodes.hpp"
#include "../../server.hpp"

namespace Skree {
    struct out_data_c_ctx {
        Utils::known_event_t* event;
        Utils::muh_str_t* rin;
        Utils::muh_str_t* rpr;
        uint64_t rid;
        uint64_t failover_key_len;
        char* failover_key;
    };

    namespace PendingReads {
        namespace Callbacks {
            class ReplicationPingTask : public Skree::Base::PendingRead::Callback {
            public:
                ReplicationPingTask(Skree::Server& _server)
                    : Skree::Base::PendingRead::Callback(_server) {};

                virtual Skree::Base::PendingWrite::QueueItem* run(
                    Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item,
                    Skree::Base::PendingRead::Callback::Args& args
                ) override;

                virtual void error(
                    Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item
                ) override;
            };
        }
    }
}
