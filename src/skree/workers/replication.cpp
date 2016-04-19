// #include "replication.hpp"

namespace Skree {
    namespace Workers {
        void Replication::run() {
            while(true) {
                std::vector<muh_str_t*> peer_ids;

                pthread_mutex_lock(server->peers_to_discover_mutex);

                for(
                    peers_to_discover_t::const_iterator it = server->peers_to_discover.cbegin();
                    it != server->peers_to_discover.cend();
                    ++it
                ) {
                    muh_str_t* item = (muh_str_t*)malloc(sizeof(*item));

                    item->len = strlen(it->first);
                    item->data = it->first;

                    peer_ids.push_back(item);
                }

                pthread_mutex_unlock(server->peers_to_discover_mutex);

                std::random_shuffle(peer_ids.begin(), peer_ids.end());

                known_peers_t::const_iterator _peer;
                Skree::Client* peer;
                get_keys_result_t* dbdata;
                std::vector<std::string> keys;
                uint64_t now = std::time(nullptr);

                for(
                    known_events_t::const_iterator _event = server->known_events.cbegin();
                    _event != server->known_events.cend();
                    ++_event
                ) {
                    known_event_t* event = _event->second;
                    // printf("repl thread: %s\n", event->id);

                    for(
                        std::vector<muh_str_t*>::const_iterator _peer_id = peer_ids.cbegin();
                        _peer_id != peer_ids.cend();
                        ++_peer_id
                    ) {
                        size_t suffix_len =
                            event->id_len
                            + 1 // :
                            + (*_peer_id)->len
                        ;

                        char* suffix = (char*)malloc(
                            suffix_len
                            + 1 // :
                            + 20 // wrinseq
                            + 1 // \0
                        );
                        sprintf(suffix, "%s:%s", event->id, (*_peer_id)->data);
                        // printf("repl thread: %s\n", suffix);

                        std::string wrinseq_key("wrinseq:", 8);
                        wrinseq_key.append(suffix, suffix_len);

                        std::string rinseq_key("rinseq:", 7);
                        rinseq_key.append(suffix, suffix_len);

                        keys.push_back(wrinseq_key);
                        keys.push_back(rinseq_key);
        // printf("replication_thread: before first db_get_keys\n");
                        dbdata = server->db->db_get_keys(keys);
                        // printf("replication_thread: after first db_get_keys\n");
                        keys.clear();

                        if(dbdata == NULL) {
                            fprintf(stderr, "db.accept_bulk failed: %s\n", db.error().name());
                            exit(1);
                        }

                        uint64_t rinseq;
                        uint64_t wrinseq;

                        auto next = [&wrinseq_key, &wrinseq](){
                            uint64_t next_wrinseq = htonll(wrinseq + 1);
                            uint64_t __wrinseq = htonll(wrinseq);
        // printf("replication_thread: before db.cas()\n");
                            if(!db.cas(
                                wrinseq_key.c_str(),
                                wrinseq_key.length(),
                                (char*)&__wrinseq,
                                sizeof(__wrinseq),
                                (char*)&next_wrinseq,
                                sizeof(next_wrinseq)
                            )) {
                                fprintf(
                                    stderr, "db.cas(%s,%llu,%llu) failed: %s\n",
                                    wrinseq_key.c_str(), wrinseq,
                                    ntohll(next_wrinseq),
                                    server->db->error().name()
                                );
                                exit(1);
                            }
        // printf("replication_thread: after db.cas()\n");
                        };

                        {
                            uint64_t* _rinseq = server->db->parse_db_value<uint64_t>(dbdata, &rinseq_key);
                            uint64_t* _wrinseq = server->db->parse_db_value<uint64_t>(dbdata, &wrinseq_key);

                            if(_rinseq == NULL) rinseq = 0;
                            else {
                                rinseq = ntohll(*_rinseq);
                                free(_rinseq);
                            }

                            if(_wrinseq == NULL) {
                                wrinseq = 0;
                                uint64_t __wrinseq = htonll(wrinseq);

                                if(!db.add(
                                    wrinseq_key.c_str(),
                                    wrinseq_key.length(),
                                    (char*)&__wrinseq,
                                    sizeof(__wrinseq)
                                )) {
                                    auto error = server->db->error();
                                    fprintf(
                                        stderr, "db.add(%s) failed: %s\n",
                                        wrinseq_key.c_str(), error.name()
                                    );

                                    if(error.code() == kyotocabinet::BasicDB::Error::Code::DUPREC) {
                                        next();
                                        continue;

                                    } else {
                                        exit(1);
                                    }
                                }

                            } else {
                                wrinseq = ntohll(*_wrinseq);
                                free(_wrinseq);
                            }
                        }

                        delete dbdata;

                        if(wrinseq >= rinseq) {
                            // printf("Skip repl: %llu >= %llu\n", wrinseq, rinseq);
                            free(suffix);
                            continue;
                        }

                        suffix[suffix_len] = ':';
                        ++suffix_len;

                        sprintf(suffix + suffix_len, "%llu", wrinseq);
                        suffix_len += 20;
                        size_t suffix_slen = strlen(suffix);

                        std::string rin_key("rin:", 4);
                        rin_key.append(suffix, suffix_slen);

                        std::string rts_key("rts:", 4);
                        rts_key.append(suffix, suffix_slen);

                        std::string rid_key("rid:", 4);
                        rid_key.append(suffix, suffix_slen);

                        std::string rpr_key("rpr:", 4);
                        rpr_key.append(suffix, suffix_slen);

                        keys.push_back(rin_key);
                        keys.push_back(rts_key);
                        keys.push_back(rid_key);
                        keys.push_back(rpr_key);

                        // for(
                        //     std::vector<std::string>::const_iterator it = keys.cbegin();
                        //     it != keys.cend();
                        //     ++it
                        //) {
                        //
                        //     printf("gotta ask for (%lu bytes) %s\n", it->size(), it->c_str());
                        // }

                        dbdata = server->db->db_get_keys(keys);
                        keys.clear();

                        if(dbdata == NULL) {
                            fprintf(stderr, "db.accept_bulk failed: %s\n", server->db->error().name());
                            exit(1);
                        }

                        uint32_t rin_len;
                        size_t _rin_len;
                        char* rin = server->db->parse_db_value<char>(dbdata, &rin_key, &_rin_len);
                        rin_len = _rin_len;

                        if(rin == NULL) {
                            fprintf(stderr, "No data for replicated event: %s, rin_key: %s\n", suffix, rin_key.c_str());
                            next();
                            continue;
                        }

                        uint64_t rts;
                        uint64_t rid;
                        uint64_t rid_net;

                        {
                            uint64_t* _rts = server->db->parse_db_value<uint64_t>(dbdata, &rts_key);
                            uint64_t* _rid = server->db->parse_db_value<uint64_t>(dbdata, &rid_key);

                            if(_rts == NULL) {
                                fprintf(stderr, "No timestamp for replicated event: %s\n", suffix);
                                next();
                                continue;

                            } else {
                                rts = ntohll(*_rts);
                                // free(_rts);
                            }

                            if(_rid == NULL) {
                                fprintf(stderr, "No remote id for replicated event: %s\n", suffix);
                                next();
                                continue;

                            } else {
                                rid_net = *_rid;
                                // free(_rid);
                                rid = ntohll(rid_net);
                            }
                        }

                        size_t rpr_len;
                        char* rpr = server->db->parse_db_value<char>(dbdata, &rpr_key, &rpr_len);

                        delete dbdata;

                        if((rts + event->ttl) > now) {
                            // printf("skip repl: not now\n");
                            free(rin);
                            if(rpr != NULL) free(rpr);
                            free(suffix);
                            continue;
                        }

                        char* failover_key = suffix;
                        sprintf(failover_key + suffix_len - 20 - 1, "%llu", rid);

                        {
                            failover_t::const_iterator it = server->failover->find(failover_key);

                            if(it != server->failover->cend()) {
                                free(rin);
                                if(rpr != NULL) free(rpr);
                                // free(suffix);
                                free(failover_key);
                                continue;
                            }
                        }

                        {
                            no_failover_t::const_iterator it = server->no_failover->find(failover_key);

                            if(it != server->no_failover->cend()) {
                                if((it->second + server->no_failover_time) > now) {
                                    free(rin);
                                    if(rpr != NULL) free(rpr);
                                    // free(suffix);
                                    free(failover_key);
                                    continue;

                                } else {
                                    server->no_failover->erase(it);
                                }
                            }
                        }

                        server->failover[failover_key] = 0;

                        pthread_mutex_lock(server->known_peers_mutex);

                        _peer = server->known_peers->find((*_peer_id)->data);

                        if(_peer == server->known_peers->cend()) peer = NULL;
                        else peer = _peer->second;

                        pthread_mutex_unlock(server->known_peers_mutex);

                        // printf("Seems like I need to failover task %llu\n", rid);

                        if(peer == NULL) {
                            size_t offset = 0;
                            uint32_t peers_cnt = 0;
                            bool have_rpr = false;

                            uint32_t* count_replicas = (uint32_t*)malloc(sizeof(
                                *count_replicas));
                            uint32_t* acceptances = (uint32_t*)malloc(sizeof(
                                *acceptances));
                            uint32_t* pending = (uint32_t*)malloc(sizeof(
                                *pending));

                            *count_replicas = 0;
                            *acceptances = 0;
                            *pending = 0;

                            pthread_mutex_t* mutex = (pthread_mutex_t*)malloc(sizeof(*mutex));
                            pthread_mutex_init(mutex, NULL);

                            muh_str_t* data_str = (muh_str_t*)malloc(sizeof(*data_str));

                            data_str->len = rin_len;
                            data_str->data = rin;

                            if(rpr != NULL) {
                                uint32_t _peers_cnt;
                                memcpy(&_peers_cnt, rpr + offset, sizeof(_peers_cnt));
                                peers_cnt = ntohl(_peers_cnt);

                                *count_replicas = peers_cnt;

                                auto i_req = Skree::Actions::I::out_init(
                                    *peer_id, event, rid_net);

                                if(peers_cnt > 0) {
                                    have_rpr = true;

                                    while(peers_cnt > 0) {
                                        size_t peer_id_len = strlen(rpr + offset); // TODO: get rid of this shit
                                        char* peer_id = (char*)malloc(peer_id_len + 1);
                                        memcpy(peer_id, rpr + offset, peer_id_len);
                                        peer_id[peer_id_len] = '\0';
                                        offset += peer_id_len + 1;

                                        known_peers_t::const_iterator it = server->known_peers->find(peer_id);

                                        if(it == server->known_peers->cend()) {
                                            ++(*acceptances);

                                        } else {
                                            ++(*pending);

                                            out_packet_i_ctx* ctx = (out_packet_i_ctx*)malloc(
                                                sizeof(*ctx));

                                            ctx->count_replicas = count_replicas;
                                            ctx->pending = pending;
                                            ctx->acceptances = acceptances;
                                            ctx->mutex = mutex;
                                            ctx->event = event;
                                            ctx->data = data_str;
                                            ctx->peer_id = *_peer_id;
                                            ctx->wrinseq = wrinseq;
                                            ctx->failover_key = failover_key;
                                            ctx->failover_key_len = suffix_len;
                                            ctx->rpr = rpr;
                                            ctx->peers_cnt = peers_cnt;
                                            ctx->rid = rid;

                                            const Skree::PendingReads::Callbacks::Replication::ProposeSelf cb (server);
                                            const Skree::Base::PendingRead::QueueItem item {
                                                .len = 1,
                                                .cb = std::move(cb),
                                                .ctx = (void*)ctx,
                                                .opcode = true
                                            };

                                            Skree::Base::PendingWrite::QueueItem witem (
                                                .len = i_req->len,
                                                .data = i_req->data,
                                                .pos = 0,
                                                .cb = std::move(item)
                                            );

                                            it->second->push_write_queue(std::move(witem));
                                        }

                                        --peers_cnt;
                                    }
                                }
                            }

                            if(!have_rpr) {
                                out_packet_i_ctx* ctx = (out_packet_i_ctx*)malloc(sizeof(*ctx));

                                ctx->count_replicas = count_replicas;
                                ctx->pending = pending;
                                ctx->acceptances = acceptances;
                                ctx->mutex = mutex;
                                ctx->event = event;
                                ctx->data = data_str;
                                ctx->peer_id = *_peer_id;
                                ctx->wrinseq = wrinseq;
                                ctx->failover_key = failover_key;
                                ctx->failover_key_len = suffix_len;
                                ctx->rpr = rpr;
                                ctx->peers_cnt = 0;
                                ctx->rid = rid;

                                // TODO: replication_exec_queue.push(ctx);
                                server->push_replication_exec_queue(ctx);
                            }

                        } else {
                            // TODO: rin_str's type
                            muh_str_t* rin_str = (muh_str_t*)malloc(sizeof(*rin_str));

                            rin_str->len = rin_len;
                            rin_str->data = rin;

                            muh_str_t* rpr_str = NULL;

                            if(rpr != NULL) {
                                rpr_str = (muh_str_t*)malloc(sizeof(*rpr_str));
                                rpr_str->len = strlen(rpr);
                                rpr_str->data = rpr;
                            }

                            out_data_c_ctx* ctx = (out_data_c_ctx*)malloc(sizeof(*ctx));

                            ctx->event = event;
                            ctx->rin = rin_str;
                            ctx->rpr = rpr_str;
                            ctx->rid = rid;
                            ctx->wrinseq = wrinseq;
                            ctx->failover_key = failover_key;
                            ctx->failover_key_len = suffix_len;

                            const Skree::PendingReads::Callbacks::Replication::PingTask cb (server);
                            const Skree::Base::PendingRead::QueueItem item {
                                .len = 1,
                                .cb = std::move(cb),
                                .ctx = (void*)ctx,
                                .opcode = true
                            };

                            auto c_req = Skree::Actions::C::out_init(event, rid_net, rin_len, rin);

                            Skree::Base::PendingWrite::QueueItem witem (
                                .len = c_req->len,
                                .data = c_req->data,
                                .pos = 0,
                                .cb = std::move(item)
                            );

                            peer->push_write_queue(std::move(witem));
                        }

                        next();
                    }
                }
            }
        }
    }
}
