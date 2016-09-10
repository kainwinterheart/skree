#include "replication.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            std::shared_ptr<Skree::Base::PendingWrite::QueueItem> Replication::run(
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item,
                std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args
            ) {
                std::shared_ptr<out_packet_r_ctx> ctx (item.ctx, (out_packet_r_ctx*)item.ctx.get());
                --(ctx->pending);

                if(args->opcode == SKREE_META_OPCODE_K) {
                    std::shared_ptr<packet_r_ctx_peer> peer;
                    peer.reset(new packet_r_ctx_peer {
                        .hostname_len = client.get_peer_name_len(),
                        .hostname = client.get_peer_name(),
                        .port = htonl(client.get_peer_port())
                    });

                    ctx->accepted_peers->push_back(peer);
                }

                server.begin_replication(ctx);

                return std::shared_ptr<Skree::Base::PendingWrite::QueueItem>();
            }

            void Replication::error(
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item
            ) {
                std::shared_ptr<out_packet_r_ctx> ctx (item.ctx, (out_packet_r_ctx*)item.ctx.get());
                --(ctx->pending);

                server.begin_replication(ctx);
            }
        }
    }
}
