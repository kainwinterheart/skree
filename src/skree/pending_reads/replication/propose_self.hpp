#pragma once
#include "../../base/pending_read.hpp"
#include "../../actions/r.hpp"
#include "../../meta/opcodes.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            class ReplicationProposeSelf : public Skree::Base::PendingRead::Callback {
            private:
                void continue_replication_exec(out_packet_i_ctx& ctx);

            public:
                ReplicationProposeSelf(Skree::Server& _server)
                    : Skree::Base::PendingRead::Callback(_server) {};

                virtual std::shared_ptr<Skree::Base::PendingWrite::QueueItem> run(
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
