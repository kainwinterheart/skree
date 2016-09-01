#include "ping_task.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            std::shared_ptr<Skree::Base::PendingWrite::QueueItem> ReplicationPingTask::run(
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item,
                Skree::Base::PendingRead::Callback::Args& args
            ) {
                std::shared_ptr<out_data_c_ctx> ctx (item.ctx, (out_data_c_ctx*)item.ctx.get());

                if(args.opcode == SKREE_META_OPCODE_K) {
                    // Utils::cluck(2, "[ReplicationPingTask] %s: c -> k\n", ctx->failover_key);
                    auto& event = *(ctx->event);

                    server.repl_clean(ctx->failover_key_len, ctx->failover_key, event);
                    event.unfailover(ctx->failover_key);

                } else {
                    // Utils::cluck(2, "[ReplicationPingTask] %s: c -> f\n", ctx->failover_key);
                    error(client, item); // calls event.unfailover() by itself
                }

                return std::shared_ptr<Skree::Base::PendingWrite::QueueItem>();
            }

            void ReplicationPingTask::error(
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item
            ) {
                // TODO: looks like this is unnecessary
                // out_data_c_ctx* ctx = (out_data_c_ctx*)(item.ctx);
                //
                // auto event = new in_packet_r_ctx_event {
                //     .len = ctx->rin->len,
                //     .data = ctx->rin->data,
                //     .id = (char*)malloc(21),
                //     .id_net = htonll(ctx->rid)
                // };
                //
                // sprintf(event->id, "%lu", ctx->rid);
                //
                // packet_r_ctx_peer* peers [server.max_replication_factor];
                // in_packet_r_ctx_event* events [1];
                // events[0] = event;
                // uint32_t peers_count = 0;
                //
                // if(ctx->rpr != nullptr) {
                //     size_t rpr_len = ctx->rpr->len;
                //     size_t rpr_offset = 0;
                //     size_t peer_id_len;
                //     char* peer_id;
                //     char* delimiter;
                //
                //     while(rpr_offset < rpr_len) {
                //         peer_id_len = strlen(ctx->rpr->data + rpr_offset);
                //         peer_id = ctx->rpr->data + rpr_offset;
                //         rpr_offset += peer_id_len + 1;
                //         delimiter = rindex(peer_id, ':');
                //
                //         if(delimiter == nullptr) {
                //             Utils::cluck(2, "Invalid peer id: %s\n", peer_id);
                //
                //         } else {
                //             auto peer = new packet_r_ctx_peer {
                //                 .hostname_len = (uint32_t)(delimiter - peer_id),
                //                 .port = (uint32_t)atoi(delimiter + 1),
                //                 .hostname = (char*)malloc(delimiter - peer_id + 1)
                //             };
                //
                //             memcpy(peer->hostname, peer_id, peer->hostname_len);
                //             peer->hostname[peer->hostname_len] = '\0';
                //
                //             peers[ peers_count++ ] = peer;
                //
                //             if(peers_count == server.max_replication_factor) {
                //                 break;
                //             }
                //         }
                //     }
                // }
                //
                // in_packet_r_ctx r_ctx {
                //     .hostname_len = client.get_peer_name_len(),
                //     .port = client.get_peer_port(),
                //     .hostname = strndup(client.get_peer_name(), client.get_peer_name_len()),
                //     .event_name_len = ctx->event->id_len,
                //     .event_name = strndup(ctx->event->id, ctx->event->id_len),
                //     .events = events,
                //     .peers = peers,
                //     .events_count = 1,
                //     .peers_count = peers_count
                // };
                //
                // short result = server.repl_save(&r_ctx, client, *(ctx->event->r_queue));
                //
                // if(result == REPL_SAVE_RESULT_K) {
                //     auto& event = *(ctx->event);
                //
                //     server.repl_clean(ctx->failover_key_len, ctx->failover_key, event);
                //     event.unfailover(ctx->failover_key);
                //
                // } else if(result == REPL_SAVE_RESULT_F) {
                //     Utils::cluck(1, "repl_save() failed\n");
                //     abort();
                //
                // } else {
                //     Utils::cluck(2, "Unexpected repl_save() result: %d\n", result);
                //     abort();
                // }
            }
        }
    }
}
