#include "ping_task.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            Skree::Base::PendingWrite::QueueItem* ReplicationPingTask::run(
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item,
                Skree::Base::PendingRead::Callback::Args& args
            ) {
                // TODO: free(ctx)
                out_data_c_ctx* ctx = (out_data_c_ctx*)(item.ctx);

                if(args.data[0] == SKREE_META_OPCODE_K) {
                    server.repl_clean(
                        ctx->failover_key_len,
                        ctx->failover_key,
                        ctx->rid
                    );

                    server.unfailover(ctx->failover_key);

                } else {
                    error(client, item); // calls server.unfailover() by itself
                }

                return NULL;
            }

            void ReplicationPingTask::error(
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item
            ) {
                out_data_c_ctx* ctx = (out_data_c_ctx*)(item.ctx);

                auto event = new in_packet_r_ctx_event {
                    .len = ctx->rin->len,
                    .data = ctx->rin->data,
                    .id = (char*)malloc(21),
                    .id_net = htonll(ctx->rid)
                };

                sprintf(event->id, "%llu", ctx->rid);

                packet_r_ctx_peer* peers [server.max_replication_factor];
                in_packet_r_ctx_event* events [1];
                events[0] = event;
                uint32_t peers_count = 0;

                if(ctx->rpr != NULL) {
                    size_t rpr_len = ctx->rpr->len;
                    size_t rpr_offset = 0;
                    size_t peer_id_len;
                    char* peer_id;
                    char* delimiter;

                    while(rpr_offset < rpr_len) {
                        peer_id_len = strlen(ctx->rpr->data + rpr_offset);
                        peer_id = ctx->rpr->data + rpr_offset;
                        rpr_offset += peer_id_len + 1;
                        delimiter = rindex(peer_id, ':');

                        if(delimiter == NULL) {
                            fprintf(stderr, "Invalid peer id: %s\n", peer_id);

                        } else {
                            auto peer = new packet_r_ctx_peer {
                                .hostname_len = (uint32_t)(delimiter - peer_id),
                                .port = (uint32_t)atoi(delimiter + 1),
                                .hostname = (char*)malloc(delimiter - peer_id + 1)
                            };

                            memcpy(peer->hostname, peer_id, peer->hostname_len);
                            peer->hostname[peer->hostname_len] = '\0';

                            peers[ peers_count++ ] = peer;

                            if(peers_count == server.max_replication_factor) {
                                break;
                            }
                        }
                    }
                }

                in_packet_r_ctx r_ctx {
                    .hostname_len = client.get_peer_name_len(),
                    .port = client.get_peer_port(),
                    .hostname = strndup(client.get_peer_name(), client.get_peer_name_len()),
                    .event_name_len = ctx->event->id_len,
                    .event_name = strndup(ctx->event->id, ctx->event->id_len),
                    .events = events,
                    .peers = peers,
                    .events_count = 1,
                    .peers_count = peers_count
                };

                short result = server.repl_save(&r_ctx, client, *(ctx->event->r_queue));

                if(result == REPL_SAVE_RESULT_K) {
                    server.repl_clean(
                        ctx->failover_key_len,
                        ctx->failover_key,
                        ctx->rid
                    );

                } else if(result == REPL_SAVE_RESULT_F) {
                    fprintf(stderr, "repl_save() failed\n");
                    exit(1);

                } else {
                    fprintf(stderr, "Unexpected repl_save() result: %d\n", result);
                    exit(1);
                }

                server.unfailover(ctx->failover_key);
            }
        }
    }
}
