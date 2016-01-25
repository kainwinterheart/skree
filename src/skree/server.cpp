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
        std::vector<uint64_t>* task_ids
    ) {
        short result = SAVE_EVENT_RESULT_F;

        if(replication_factor > max_replication_factor)
            replication_factor = max_replication_factor;

        uint64_t increment_key_len = 6 + ctx->event_name_len;
        char* increment_key = (char*)malloc(increment_key_len);

        sprintf(increment_key, "inseq:");
        memcpy(increment_key + 6, ctx->event_name, ctx->event_name_len);

        uint32_t _cnt = htonl(ctx->events->size());
        size_t r_len = 0;
        char* r_req = (char*)malloc(1
            + sizeof(my_hostname_len)
            + my_hostname_len
            + sizeof(my_port)
            + sizeof(ctx->event_name_len)
            + ctx->event_name_len
            + sizeof(_cnt)
        );

        r_req[0] = 'r';
        r_len += 1;

        uint32_t _hostname_len = htonl(my_hostname_len);
        memcpy(r_req + r_len, &_hostname_len, sizeof(_hostname_len));
        r_len += sizeof(_hostname_len);

        memcpy(r_req + r_len, my_hostname, my_hostname_len);
        r_len += my_hostname_len;

        uint32_t _my_port = htonl(my_port);
        memcpy(r_req + r_len, (char*)&_my_port, sizeof(_my_port));
        r_len += sizeof(_my_port);

        uint32_t _event_name_len = htonl(ctx->event_name_len);
        memcpy(r_req + r_len, (char*)&_event_name_len, sizeof(_event_name_len));
        r_len += sizeof(_event_name_len);

        memcpy(r_req + r_len, ctx->event_name, ctx->event_name_len);
        r_len += ctx->event_name_len;

        memcpy(r_req + r_len, (char*)&_cnt, sizeof(_cnt));
        r_len += sizeof(_cnt);

        uint32_t num_inserted = 0;
        bool replication_began = false;

        if(db.begin_transaction()) {
            int64_t max_id = db.increment(
                increment_key,
                increment_key_len,
                ctx->events->size(),
                0
            );

            if(max_id == kyotocabinet::INT64MIN) {
                fprintf(stderr, "Increment failed: %s\n", db.error().name());

                if(!db.end_transaction(false)) {
                    fprintf(stderr, "Failed to abort transaction: %s\n", db.error().name());
                    exit(1);
                }

            } else {
                for(
                    std::list<in_packet_e_ctx_event*>::const_iterator it =
                        ctx->events->cbegin();
                    it != ctx->events->cend();
                    ++it
                ) {
                    in_packet_e_ctx_event* event = *it;

                    event->id = (char*)malloc(21);
                    sprintf(event->id, "%llu", max_id);

                    if(task_ids != NULL)
                        task_ids->push_back(max_id);

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

                    uint64_t _max_id = htonll(max_id);

                    r_req = (char*)realloc(r_req,
                        r_len
                        + sizeof(_max_id)
                        + sizeof(event->len)
                        + event->len
                    );

                    memcpy(r_req + r_len, (char*)&_max_id, sizeof(_max_id));
                    r_len += sizeof(_max_id);

                    uint32_t _event_len = htonl(event->len);
                    memcpy(r_req + r_len, (char*)&_event_len, sizeof(_event_len));
                    r_len += sizeof(_event_len);

                    memcpy(r_req + r_len, event->data, event->len);
                    r_len += event->len;

                    --max_id;
                }

                if(num_inserted == ctx->events->size()) {
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

                        out_data_r_ctx* r_ctx =
                            (out_data_r_ctx*)malloc(sizeof(*r_ctx));

                        r_ctx->sync = (replication_factor > 0);
                        r_ctx->replication_factor = replication_factor;
                        r_ctx->pending = 0;
                        r_ctx->client = client;
                        r_ctx->candidate_peer_ids = candidate_peer_ids;
                        r_ctx->accepted_peers = accepted_peers;
                        r_ctx->r_req = r_req;
                        r_ctx->r_len = r_len;
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
        if(!replication_began) free(r_req);

        return result;
    }
}
