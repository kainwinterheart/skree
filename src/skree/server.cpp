#include "server.hpp"

namespace Skree {
    short Server::repl_save(
        in_packet_r_ctx* ctx,
        Client* client
    ) {
        short result = REPL_SAVE_RESULT_F;

        char* _peer_id = client->get_peer_id();
        size_t _peer_id_len = strlen(_peer_id);

        uint64_t increment_key_len = 7 + ctx->event_name_len;
        char* increment_key = (char*)malloc(increment_key_len + 1 + _peer_id_len);

        sprintf(increment_key, "rinseq:");
        memcpy(increment_key + 7, ctx->event_name, ctx->event_name_len);
        increment_key[increment_key_len] = ':';
        ++increment_key_len;
        memcpy(increment_key + increment_key_len, _peer_id, _peer_id_len);
        increment_key_len += _peer_id_len;

        uint32_t num_inserted = 0;

        uint32_t peers_cnt = ctx->peers->size();
        uint32_t _peers_cnt = htonl(peers_cnt);
        uint64_t serialized_peers_len = sizeof(_peers_cnt);
        char* serialized_peers = (char*)malloc(serialized_peers_len);
        memcpy(serialized_peers, &_peers_cnt, serialized_peers_len);

        for(
            std::list<packet_r_ctx_peer*>::const_iterator it =
                ctx->peers->cbegin();
            it != ctx->peers->cend();
            ++it
        ) {
            packet_r_ctx_peer* peer = *it;
            char* _peer_id = make_peer_id(peer->hostname_len,
                peer->hostname, peer->port);

            bool keep_peer_id = false;
            size_t _peer_id_len = strlen(_peer_id);

            pthread_mutex_lock(&peers_to_discover_mutex);

            peers_to_discover_t::const_iterator prev_item =
                peers_to_discover.find(_peer_id);

            if(prev_item == peers_to_discover.cend()) {
                peer_to_discover_t* peer_to_discover = (peer_to_discover_t*)malloc(
                    sizeof(*peer_to_discover));

                peer_to_discover->host = peer->hostname;
                peer_to_discover->port = peer->port;

                peers_to_discover[_peer_id] = peer_to_discover;
                keep_peer_id = true;
                save_peers_to_discover();
            }

            pthread_mutex_unlock(&peers_to_discover_mutex);

            serialized_peers = (char*)realloc(serialized_peers,
                serialized_peers_len + _peer_id_len);

            memcpy(serialized_peers + serialized_peers_len,
                _peer_id, _peer_id_len);
            serialized_peers_len += _peer_id_len;

            if(!keep_peer_id) free(_peer_id);
        }
    // printf("repl_save: before begin_transaction\n");
        if(db.begin_transaction()) {
    // printf("repl_save: after begin_transaction\n");
            int64_t max_id = db.increment(
                increment_key,
                increment_key_len,
                ctx->events->size(),
                0
            );

            if(max_id == kyotocabinet::INT64MIN) {
                if(!db.end_transaction(false)) {
                    fprintf(stderr, "Failed to abort transaction: %s\n", db.error().name());
                    exit(1);
                }

            } else {
                uint64_t _now = htonll(std::time(nullptr));
                size_t now_len = sizeof(_now);
                char* now = (char*)malloc(now_len);
                memcpy(now, &_now, now_len);

                for(
                    std::list<in_packet_r_ctx_event*>::const_iterator it =
                        ctx->events->cbegin();
                    it != ctx->events->cend();
                    ++it
                ) {
                    in_packet_r_ctx_event* event = *it;

                    char* r_id = (char*)malloc(21);
                    sprintf(r_id, "%llu", max_id);

                    size_t r_id_len = strlen(r_id);
                    uint32_t key_len =
                        4 // rin: | rts: | rid: | rpr:
                        + ctx->event_name_len
                        + 1 // :
                        + _peer_id_len
                        + 1 // :
                        + r_id_len
                    ;
                    char* key = (char*)malloc(key_len);

                    sprintf(key, "rin:");
                    memcpy(key + 4, ctx->event_name, ctx->event_name_len);
                    key[4 + ctx->event_name_len] = ':';

                    memcpy(key + 4 + ctx->event_name_len + 1, _peer_id,
                        _peer_id_len);
                    key[4 + ctx->event_name_len + 1 + _peer_id_len] = ':';

                    memcpy(key + 4 + ctx->event_name_len + 1 + _peer_id_len + 1,
                        r_id, r_id_len);

                    if(db.add(key, key_len, event->data, event->len)) {
                        key[1] = 't';
                        key[2] = 's';

                        if(db.add(key, key_len, now, now_len)) {
                            key[1] = 'i';
                            key[2] = 'd';

                            // printf("about to save rid: %llu\n",ntohll(event->id_net));

                            if(db.add(key, key_len, (char*)&(event->id_net),
                                sizeof(event->id_net))) {
                                bool rpr_ok = false;

                                if(peers_cnt > 0) {
                                    key[1] = 'p';
                                    key[2] = 'r';

                                    rpr_ok = db.add(
                                        key, key_len,
                                        serialized_peers,
                                        serialized_peers_len
                                    );

                                    if(!rpr_ok)
                                        fprintf(stderr, "db.add(%s) failed: %s\n", key, db.error().name());

                                } else {
                                    rpr_ok = true;
                                }

                                if(rpr_ok) {
                                    key[1] = 'r';
                                    key[2] = 'e';

                                    size_t event_id_len = strlen(event->id);
                                    key_len -= r_id_len;
                                    key = (char*)realloc(key, key_len + event_id_len);

                                    memcpy(key + key_len, event->id, event_id_len);
                                    key_len += event_id_len;

                                    auto _max_id = htonll(max_id);
    // printf("repl_save: before db.add(%s)\n", key);
                                    if(db.add(key, key_len, (char*)&_max_id, sizeof(_max_id))) {
                                        free(key);
                                        free(r_id);
                                        ++num_inserted;
                                        --max_id;

                                    } else {
                                        fprintf(stderr, "db.add(%s) failed: %s\n", key, db.error().name());
                                        free(key);
                                        free(r_id);
                                        break;
                                    }

                                } else {
                                    free(key);
                                    free(r_id);
                                    break;
                                }

                            } else {
                                fprintf(stderr, "db.add(%s) failed: %s\n", key, db.error().name());
                                free(key);
                                free(r_id);
                                break;
                            }

                        } else {
                            fprintf(stderr, "db.add(%s) failed: %s\n", key, db.error().name());
                            free(key);
                            free(r_id);
                            break;
                        }

                    } else {
                        fprintf(stderr, "db.add(%s) failed: %s\n", key, db.error().name());
                        free(key);
                        free(r_id);
                        break;
                    }
                }

                if(num_inserted == ctx->events->size()) {
                    if(db.end_transaction(true)) {
                        pthread_mutex_lock(&stat_mutex);
                        stat_num_replications += num_inserted;
                        pthread_mutex_unlock(&stat_mutex);

                        result = REPL_SAVE_RESULT_K;

                    } else {
                        fprintf(stderr, "Failed to commit transaction: %s\n", db.error().name());
                        exit(1);
                    }

                } else {
                    if(!db.end_transaction(false)) {
                        fprintf(stderr, "Failed to abort transaction: %s\n", db.error().name());
                        exit(1);
                    }
                }

                free(now);
            }

            free(increment_key);
        }

        free(serialized_peers);

        while(!ctx->peers->empty()) {
            packet_r_ctx_peer* peer = ctx->peers->back();
            ctx->peers->pop_back();

            free(peer->hostname);
            free(peer);
        }

        free(ctx->peers);

        while(!ctx->events->empty()) {
            in_packet_r_ctx_event* event = ctx->events->back();
            ctx->events->pop_back();

            free(event->data);
            free(event);
        }

        free(ctx->events);
        free(ctx->hostname);
        free(ctx->event_name);

        return result;
    }

    short Server::save_event(
        in_packet_e_ctx* ctx,
        uint32_t replication_factor,
        Client* client,
        uint64_t* task_ids
    ) {
        short result = SAVE_EVENT_RESULT_F;

        if(replication_factor > max_replication_factor)
            replication_factor = max_replication_factor;

        uint64_t increment_key_len = 6 + ctx->event_name_len;
        char* increment_key = (char*)malloc(increment_key_len);

        sprintf(increment_key, "inseq:");
        memcpy(increment_key + 6, ctx->event_name, ctx->event_name_len);

        auto r_req = Actions::R::out_init(
            this,
            event_name_len,
            event_name,
            ctx->cnt
        );
        bool replication_began = false;

        if(db.begin_transaction()) {
            int64_t max_id = db.increment(
                increment_key,
                increment_key_len,
                ctx->cnt,
                0
            );

            if(max_id == kyotocabinet::INT64MIN) {
                fprintf(stderr, "Increment failed: %s\n", db.error().name());

                if(!db.end_transaction(false)) {
                    fprintf(stderr, "Failed to abort transaction: %s\n", db.error().name());
                    exit(1);
                }

            } else {
                uint32_t _cnt = ctx->cnt;
                uint32_t num_inserted = 0;

                while(_cnt > 0) {
                    --_cnt;
                    in_packet_e_ctx_event* event = ctx->events[_cnt];

                    event->id = (char*)malloc(21);
                    sprintf(event->id, "%llu", max_id);

                    if(task_ids != NULL)
                        task_ids[_cnt] = max_id;

                    uint32_t key_len =
                        3 // in:
                        + ctx->event_name_len
                        + 1 // :
                        + strlen(event->id)
                    ;
                    char* key = (char*)malloc(key_len);

                    sprintf(key, "in:");
                    memcpy(key + 3, ctx->event_name, ctx->event_name_len);
                    key[3 + ctx->event_name_len] = ':';
                    memcpy(key + 3 + ctx->event_name_len + 1, event->id,
                        strlen(event->id));

                    if(db.add(key, key_len, event->data, event->len)) {
                        free(key);
                        ++num_inserted;

                    } else {
                        fprintf(stderr, "db.add(%s) failed: %s\n", key, db.error().name());
                        free(key);
                        break;
                    }

                    Actions::R::out_add_event(r_req, max_id, event->len, event->data);

                    --max_id;
                }

                if(num_inserted == ctx->cnt) {
                    if(db.end_transaction(true)) {
                        pthread_mutex_lock(&stat_mutex);
                        stat_num_inserts += num_inserted;
                        pthread_mutex_unlock(&stat_mutex);

                        /*****************************/
                        std::vector<char*>* candidate_peer_ids = new std::vector<char*>();
                        std::list<packet_r_ctx_peer*>* accepted_peers =
                            new std::list<packet_r_ctx_peer*>();

                        pthread_mutex_lock(&known_peers_mutex);

                        for(
                            known_peers_t::const_iterator it = known_peers.cbegin();
                            it != known_peers.cend();
                            ++it
                        ) {
                            candidate_peer_ids->push_back(it->first);
                        }

                        pthread_mutex_unlock(&known_peers_mutex);

                        std::random_shuffle(
                            candidate_peer_ids->begin(),
                            candidate_peer_ids->end()
                        );

                        out_packet_r_ctx* r_ctx =
                            (out_packet_r_ctx*)malloc(sizeof(*r_ctx));

                        r_ctx->sync = (replication_factor > 0);
                        r_ctx->replication_factor = replication_factor;
                        r_ctx->pending = 0;
                        r_ctx->client = client;
                        r_ctx->candidate_peer_ids = candidate_peer_ids;
                        r_ctx->accepted_peers = accepted_peers;
                        // TODO: use muh_str_t for r_req
                        r_ctx->r_req = r_req->data;
                        r_ctx->r_len = r_req->len;
                        /*****************************/

                        if(r_ctx->sync) {
                            if(candidate_peer_ids->size() > 0) {
                                result = SAVE_EVENT_RESULT_NULL;

                                begin_replication(r_ctx);
                                replication_began = true;

                            } else {
                                result = SAVE_EVENT_RESULT_A;
                            }

                        } else {
                            result = SAVE_EVENT_RESULT_K;

                            if(candidate_peer_ids->size() > 0) {
                                begin_replication(r_ctx);
                                replication_began = true;
                            }
                        }

                    } else {
                        fprintf(stderr, "Failed to commit transaction: %s\n",
                            db.error().name());
                        exit(1);
                    }

                } else {
                    fprintf(stderr, "Batch insert failed\n");

                    if(!db.end_transaction(false)) {
                        fprintf(stderr, "Failed to abort transaction: %s\n",
                            db.error().name());
                        exit(1);
                    }
                }
            }

        } else {
            fprintf(stderr, "Failed to start transaction: %s\n", db.error().name());
            exit(1);
        }

        free(increment_key);
        // TODO: if(!replication_began) free(r_req);

        return result;
    }

    void Server::repl_clean(
        size_t failover_key_len,
        const char* failover_key,
        uint64_t wrinseq
    ) {
        size_t failover_key_slen = strlen(failover_key);
        std::vector<std::string> keys;

        std::string rre_key("rre:", 4);
        rre_key.append(failover_key, failover_key_slen);

        keys.push_back(rre_key);

        char* suffix = (char*)malloc(failover_key_len);
        memcpy(suffix, failover_key, failover_key_len);
        sprintf(suffix + failover_key_len - 20 - 1, "%llu", wrinseq);
        failover_key_slen = strlen(suffix);

        std::string rin_key("rin:", 4);
        rin_key.append(suffix, failover_key_len);

        std::string rts_key("rts:", 4);
        rts_key.append(suffix, failover_key_len);

        std::string rid_key("rid:", 4);
        rid_key.append(suffix, failover_key_len);

        std::string rpr_key("rpr:", 4);
        rpr_key.append(suffix, failover_key_len);

        keys.push_back(rin_key);
        keys.push_back(rts_key);
        keys.push_back(rid_key);
        keys.push_back(rpr_key);

        if(db.remove_bulk(keys) == -1)
            fprintf(stderr, "db.remove_bulk failed: %s\n", db.error().name());
    }

    void Server::begin_replication(out_packet_r_ctx*& r_ctx) {
        Client* peer = NULL;

        while((peer == NULL) && (r_ctx->candidate_peer_ids->size() > 0)) {
            char* peer_id = r_ctx->candidate_peer_ids->back();
            r_ctx->candidate_peer_ids->pop_back();

            pthread_mutex_lock(&known_peers_mutex);

            known_peers_t::const_iterator it = known_peers.find(peer_id);

            if(it != known_peers.cend())
                peer = it->second;

            pthread_mutex_unlock(&known_peers_mutex);
        }

        bool done = false;

        if(peer == NULL) {
            if(r_ctx->sync) {
                uint32_t accepted_peers_count = r_ctx->accepted_peers->size();

                if(accepted_peers_count >= r_ctx->replication_factor) {
                    if(r_ctx->client != NULL) {
                        char* r_ans = (char*)malloc(1);
                        r_ans[0] = SKREE_META_OPCODE_K;
                        r_ctx->client->push_write_queue(1, r_ans, NULL);
                    }

                    r_ctx->sync = false;

                } else if(r_ctx->pending == 0) {
                    if(r_ctx->client != NULL) {
                        char* r_ans = (char*)malloc(1);
                        r_ans[0] = SKREE_META_OPCODE_A;
                        r_ctx->client->push_write_queue(1, r_ans, NULL);
                    }

                    r_ctx->sync = false;
                }
            }

            if(r_ctx->pending == 0)
                done = true;

        } else {
            uint32_t accepted_peers_count = r_ctx->accepted_peers->size();

            if(
                r_ctx->sync
                && (accepted_peers_count >= r_ctx->replication_factor)
            ) {
                if(r_ctx->client != NULL) {
                    char* r_ans = (char*)malloc(1);
                    r_ans[0] = SKREE_META_OPCODE_K;
                    r_ctx->client->push_write_queue(1, r_ans, NULL);
                }

                r_ctx->sync = false;
            }

            if(accepted_peers_count >= max_replication_factor) {
                done = true;

            } else {
                size_t r_len = 0;
                char* r_req = (char*)malloc(r_ctx->r_len + sizeof(accepted_peers_count));

                memcpy(r_req + r_len, r_ctx->r_req, r_ctx->r_len);
                r_len += r_ctx->r_len;

                uint32_t _accepted_peers_count = htonl(accepted_peers_count);
                memcpy(r_req + r_len, &_accepted_peers_count, sizeof(_accepted_peers_count));
                r_len += sizeof(_accepted_peers_count);

                for(
                    std::list<packet_r_ctx_peer*>::const_iterator it =
                        r_ctx->accepted_peers->cbegin();
                    it != r_ctx->accepted_peers->cend();
                    ++it
                ) {
                    packet_r_ctx_peer* peer = *it;

                    r_req = (char*)realloc(r_req, r_len
                        + sizeof(peer->hostname_len)
                        + peer->hostname_len
                        + sizeof(peer->port)
                    );

                    uint32_t _len = htonl(peer->hostname_len);
                    memcpy(r_req + r_len, &_len, sizeof(_len));
                    r_len += sizeof(_len);

                    memcpy(r_req + r_len, peer->hostname, peer->hostname_len);
                    r_len += peer->hostname_len;

                    memcpy(r_req + r_len, &(peer->port),
                        sizeof(peer->port));
                    r_len += sizeof(peer->port);
                }

                ++(r_ctx->pending);

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof(*item));

                item->len = 1;
                item->cb = &Client::replication_cb;
                item->ctx = (void*)r_ctx;
                item->err = &Client::replication_skip_peer;
                item->opcode = true;

                peer->push_write_queue(r_len, r_req, item);
            }
        }

        if(done) {
            while(!r_ctx->accepted_peers->empty()) {
                packet_r_ctx_peer* peer = r_ctx->accepted_peers->back();
                r_ctx->accepted_peers->pop_back();
                free(peer);
            }

            free(r_ctx->accepted_peers);
            free(r_ctx->candidate_peer_ids);
            free(r_ctx->r_req);
            free(r_ctx);
        }
    }
}
