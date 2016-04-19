// #include "ping_task.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            namespace Replication {
                virtual const Skree::Base::PendingRead::QueueItem&& PingTask::run(
                    const Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item,
                    const Skree::Base::PendingRead::Callback::Args& args
                ) {
                    out_data_c_ctx* ctx = (out_data_c_ctx*)(item.ctx);

                    if(args->data[0] == SKREE_META_OPCODE_K) {
                        server.repl_clean(
                            ctx->failover_key_len,
                            ctx->failover_key,
                            ctx->wrinseq
                        );

                        server.unfailover(ctx->failover_key);

                    } else {
                        error(client, item); // calls server.unfailover() by itself
                    }

                    // pthread_mutex_lock(ctx->mutex);
                    //
                    // pthread_mutex_unlock(ctx->mutex);

                    return Skree::PendingReads::noop(server);
                }

                virtual void PingTask::error(
                    const Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item
                ) {
                    out_data_c_ctx* ctx = (out_data_c_ctx*)(item.ctx);
                    in_packet_r_ctx* r_ctx = (in_packet_r_ctx*)malloc(sizeof(*r_ctx));

                    r_ctx->hostname_len = client.get_peer_name_len();
                    r_ctx->port = client.get_peer_port();
                    r_ctx->hostname = strndup(client.get_peer_name(), r_ctx->hostname_len);
                    r_ctx->event_name_len = ctx->event->id_len;
                    r_ctx->event_name = strndup(ctx->event->id, r_ctx->event_name_len);
                    r_ctx->events = new std::list<in_packet_r_ctx_event*>();
                    r_ctx->cnt = 0;
                    r_ctx->peers = new std::list<packet_r_ctx_peer*>();

                    in_packet_r_ctx_event* event = (in_packet_r_ctx_event*)malloc(
                        sizeof(*event));

                    event->len = ctx->rin->len;
                    event->data = ctx->rin->data;
                    event->id = (char*)malloc(21);
                    sprintf(event->id, "%llu", ctx->rid);

                    r_ctx->events->push_back(event);

                    if(ctx->rpr != NULL) {
                        size_t rpr_len = ctx->rpr->len;
                        size_t rpr_offset = 0;

                        while(rpr_offset < rpr_len) {
                            size_t peer_id_len = strlen(ctx->rpr->data + rpr_offset);
                            char* peer_id = (char*)malloc(peer_id_len + 1);
                            memcpy(peer_id, ctx->rpr->data + rpr_offset, peer_id_len);
                            peer_id[peer_id_len] = '\0';
                            rpr_offset += peer_id_len + 1;

                            char* delimiter = rindex(peer_id, ':');

                            if(delimiter == NULL) {
                                fprintf(stderr, "Invalid peer id: %s\n", peer_id);

                            } else {
                                packet_r_ctx_peer* peer = (packet_r_ctx_peer*)malloc(
                                    sizeof(*peer));

                                peer->hostname_len = delimiter - peer_id;
                                peer->port = atoi(delimiter + 1);
                                peer->hostname = (char*)malloc(peer->hostname_len + 1);

                                memcpy(peer->hostname, peer_id, peer->hostname_len);
                                peer->hostname[peer->hostname_len] = '\0';

                                r_ctx->peers->push_back(peer);
                            }
                        }
                    }

                    short result = server.repl_save(r_ctx, client);

                    if(result == REPL_SAVE_RESULT_K) {
                        server.repl_clean(
                            ctx->failover_key_len,
                            ctx->failover_key,
                            ctx->wrinseq
                        );

                    } else if(result == REPL_SAVE_RESULT_F) {
                        fprintf(stderr, "repl_save() failed\n");
                        exit(1);

                    } else {
                        fprintf(stderr, "Unexpected repl_save() result: %d\n", result);
                        exit(1);
                    }

                    server.unfailover(ctx->failover_key);

                    // pthread_mutex_lock(ctx->mutex);
                    //
                    // pthread_mutex_unlock(ctx->mutex);
                }
            }
        }
    }
}
