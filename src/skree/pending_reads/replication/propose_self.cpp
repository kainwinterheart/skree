#include "propose_self.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            std::shared_ptr<Skree::Base::PendingWrite::QueueItem> ReplicationProposeSelf::run(
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item,
                std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args
            ) {
                std::shared_ptr<out_packet_i_ctx> ctx (item.ctx, (out_packet_i_ctx*)item.ctx.get());

                ctx->mutex->Lock();

                --(*(ctx->pending));

                if(args->opcode == SKREE_META_OPCODE_K)
                    ++(*(ctx->acceptances));

                continue_replication_exec(ctx);

                return std::shared_ptr<Skree::Base::PendingWrite::QueueItem>();
            }

            void ReplicationProposeSelf::error(
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item
            ) {
                std::shared_ptr<out_packet_i_ctx> ctx (item.ctx, (out_packet_i_ctx*)item.ctx.get());

                ctx->mutex->Lock();

                --(*(ctx->pending));

                continue_replication_exec(ctx);
            }

            void ReplicationProposeSelf::continue_replication_exec(
                std::shared_ptr<out_packet_i_ctx> ctx
            ) {
                if(*(ctx->pending) == 0) {
                    ctx->mutex->Unlock();

                    server.replication_exec(ctx);

                } else {
                    ctx->mutex->Unlock();
                }
            }
        }
    }
}
