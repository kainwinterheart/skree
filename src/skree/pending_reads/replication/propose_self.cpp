#include "propose_self.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            Skree::Base::PendingWrite::QueueItem* ReplicationProposeSelf::run(
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item,
                Skree::Base::PendingRead::Callback::Args& args
            ) {
                out_packet_i_ctx* ctx = (out_packet_i_ctx*)(item.ctx);

                pthread_mutex_lock(ctx->mutex);

                --(*(ctx->pending));

                if(args.data[0] == SKREE_META_OPCODE_K)
                    ++(*(ctx->acceptances));

                continue_replication_exec(ctx);

                pthread_mutex_unlock(ctx->mutex);

                return nullptr;
            }

            void ReplicationProposeSelf::error(
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item
            ) {
                out_packet_i_ctx* ctx = (out_packet_i_ctx*)(item.ctx);

                pthread_mutex_lock(ctx->mutex);

                --(*(ctx->pending));

                continue_replication_exec(ctx);

                pthread_mutex_unlock(ctx->mutex);
            }

            void ReplicationProposeSelf::continue_replication_exec(out_packet_i_ctx*& ctx) {
                if(*(ctx->pending) == 0) {
                    server.replication_exec(ctx);
                }
            }
        }
    }
}
