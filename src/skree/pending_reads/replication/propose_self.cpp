#include "propose_self.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            namespace Replication {
                virtual const Skree::Base::PendingRead::QueueItem&& ProposeSelf::run(
                    const Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item,
                    const Skree::Base::PendingRead::Callback::Args& args
                ) {
                    out_data_i_ctx* ctx = (out_data_i_ctx*)(item.ctx);

                    pthread_mutex_lock(ctx->mutex);

                    --(*(ctx->pending));

                    if(args->data[0] == SKREE_META_OPCODE_K)
                        ++(*(ctx->acceptances));

                    continue_replication_exec(ctx);

                    pthread_mutex_unlock(ctx->mutex);

                    return Skree::PendingReads::noop(server);
                }

                virtual void ProposeSelf::error(
                    const Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item
                ) {
                    out_data_i_ctx* ctx = (out_data_i_ctx*)(item.ctx);

                    pthread_mutex_lock(ctx->mutex);

                    --(*(ctx->pending));

                    continue_replication_exec(ctx);

                    pthread_mutex_unlock(ctx->mutex);
                }

                void ProposeSelf::continue_replication_exec(out_packet_i_ctx*& ctx) {
                    if(*(ctx->pending) == 0) {
                        pthread_mutex_lock(server.replication_exec_queue_mutex);

                        server.push_replication_exec_queue(ctx);

                        pthread_mutex_unlock(server.replication_exec_queue_mutex);
                    }
                }
            }
        }
    }
}
