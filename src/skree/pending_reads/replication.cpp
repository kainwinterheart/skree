#include "replication.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            Skree::Base::PendingWrite::QueueItem* Replication::run(
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item,
                Skree::Base::PendingRead::Callback::Args& args
            ) {
                out_packet_r_ctx* ctx = (out_packet_r_ctx*)(item.ctx);
                --(ctx->pending);

                if(args.opcode == SKREE_META_OPCODE_K) {
                    packet_r_ctx_peer* peer =
                        (packet_r_ctx_peer*)malloc(sizeof(*peer));

                    peer->hostname_len = client.get_peer_name_len();
                    peer->hostname = (char*)(client.get_peer_name());
                    peer->port = htonl(client.get_peer_port());

                    ctx->accepted_peers->push_back(peer);
                }

                server.begin_replication(ctx);

                return nullptr;
            }

            void Replication::error(
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item
            ) {
                out_packet_r_ctx* ctx = (out_packet_r_ctx*)(item.ctx);
                --(ctx->pending);

                server.begin_replication(ctx);
            }
        }
    }
}
