// #include "replication_exec.hpp"

namespace Skree {
    namespace Workers {
        void ReplicationExec::run() {
            while(true) {
                if(server->replication_exec_queue->size() == 0) continue;

                pthread_mutex_lock(server->replication_exec_queue_mutex);

                // TODO: persistent queue
                out_packet_i_ctx* ctx = server->replication_exec_queue->front();
                server->replication_exec_queue->pop();

                pthread_mutex_unlock(server->replication_exec_queue_mutex);

                // printf("Replication exec thread for task %llu\n", ctx->rid);

                if(ctx->acceptances == ctx->count_replicas) {
                    {
                        failover_t::const_iterator it =
                            server->failover->find(ctx->failover_key);

                        if(it == server->failover->cend()) {
                            // TODO: cleanup
                            continue;
                        }
                    }

                    {
                        no_failover_t::const_iterator it =
                            server->no_failover->find(ctx->failover_key);

                        if(it != server->no_failover->cend()) {
                            if((it->second + server->no_failover_time) > std::time(nullptr)) {
                                // TODO: cleanup
                                continue;

                            } else {
                                server->no_failover->erase(it);
                            }
                        }
                    }

                    in_packet_e_ctx_event* events [1];

                    events[0] = (in_packet_e_ctx_event*)malloc(sizeof(*event));
                    events[0]->len = ctx->data->len;
                    events[0]->data = ctx->data->data;
                    events[0]->id = NULL;

                    in_packet_e_ctx e_ctx {
                        .cnt = 1,
                        .event_name_len = ctx->event->id_len,
                        .event_name = ctx->event->id,
                        .events = events
                    };

                    std::vector<uint64_t> task_ids;
                    server->save_event(&e_ctx, 0, NULL, &task_ids);

                    // Client::free_in_packet_e_ctx((void*)e_ctx);

                    (*(server->failover))[ctx->failover_key] = task_ids.front();

                    // TODO: this should probably make 'i' packet return 'f'
                    server->repl_clean(
                        ctx->failover_key_len,
                        ctx->failover_key,
                        ctx->wrinseq
                    );

                    if(ctx->rpr != NULL) {
                        // const muh_str_t*& peer_id, const known_event_t*& event,
                        // const uint64_t& rid
                        auto x_req = Skree::Actions::X::out_init(
                            ctx->peer_id, ctx->event, ctx->rid);
                        size_t offset = 0;

                        while(ctx->peers_cnt > 0) {
                            size_t peer_id_len = strlen(ctx->rpr + offset);
                            char* peer_id = (char*)malloc(peer_id_len + 1);
                            memcpy(peer_id, ctx->rpr + offset, peer_id_len);
                            peer_id[peer_id_len] = '\0';
                            offset += peer_id_len + 1;

                            known_peers_t::const_iterator it =
                                server->known_peers->find(peer_id);

                            if(it != server->known_peers->cend()) {

                                Skree::Base::PendingWrite::QueueItem item (
                                    .len = x_req->len,
                                    .data = x_req->data,
                                    .pos = 0,
                                    .cb = Skree::PendingReads::noop(server)
                                );

                                it->second->push_write_queue(std::move(item));
                            }

                            --(ctx->peers_cnt);
                        }
                    }

                } else {
                    // TODO: think about repl_clean()
                    server->repl_clean(
                        ctx->failover_key_len,
                        ctx->failover_key,
                        ctx->wrinseq
                    );

                    server->unfailover(ctx->failover_key);

                    free(ctx->data->data);
                    free(ctx->data);
                }

                pthread_mutex_destroy(ctx->mutex);

                free(ctx->mutex);
                free(ctx->acceptances);
                free(ctx->pending);
                free(ctx->count_replicas);
                free(ctx);
            }
        }
    }
}
