#include "replication.hpp"

namespace Skree {
    namespace Workers {
        void Replication::run() {
            uint64_t now;
            bool active;

            while(true) {
                now = std::time(nullptr);
                active = false;

                for(auto& it : server.known_events) {
                    if(failover(now, *(it.second))) {
                        active = true;
                    }

                    if(replication(now, *(it.second))) {
                        active = true;
                    }
                }

                ++(server.stat_num_repl_it);

                if(!active) {
                    sleep(1);
                }
            }
        }

        Replication::QueueItem* Replication::parse_queue_item(
            const Utils::known_event_t& event,
            char*& item
        ) {
            auto out = new Replication::QueueItem;
            size_t item_pos = 0;

            memcpy(&(out->rin_len), item + item_pos, sizeof(out->rin_len));
            item_pos += sizeof(out->rin_len);
            out->rin_len = ntohl(out->rin_len);

            out->rin = item + item_pos;
            item_pos += out->rin_len;

            memcpy(&(out->rts), item + item_pos, sizeof(out->rts));
            item_pos += sizeof(out->rts);
            out->rts = ntohll(out->rts);

            memcpy(&(out->rid_net), item + item_pos, sizeof(out->rid_net));
            item_pos += sizeof(out->rid_net);

            out->rid = ntohll(out->rid_net);

            memcpy(&(out->hostname_len), item + item_pos, sizeof(out->hostname_len));
            item_pos += sizeof(out->hostname_len);
            out->hostname_len = ntohl(out->hostname_len);

            out->hostname = item + item_pos;
            item_pos += out->hostname_len;

            memcpy(&(out->port), item + item_pos, sizeof(out->port));
            item_pos += sizeof(out->port);
            out->port = htonl(out->port);

            memcpy(&(out->peers_cnt), item + item_pos, sizeof(out->peers_cnt));
            item_pos += sizeof(out->peers_cnt);
            out->peers_cnt = ntohl(out->peers_cnt);

            out->rpr = item + item_pos;

            out->peer_id = Utils::make_peer_id(out->hostname_len, out->hostname, out->port);
            out->peer_id_len = strlen(out->peer_id);

            out->failover_key_len =
                event.id_len
                + 1 // :
                + out->peer_id_len
            ;

            out->failover_key = (char*)malloc(
                out->failover_key_len
                + 1 // :
                + 20 // wrinseq
                + 1 // \0
            );
            sprintf(out->failover_key, "%s:%s", event.id, out->peer_id);
            // printf("repl thread: %s\n", suffix);

            (out->failover_key)[out->failover_key_len] = ':';
            ++(out->failover_key_len);

            sprintf(out->failover_key + out->failover_key_len, "%llu", out->rid);
            // suffix_len += 20;

            out->failover_key_len = strlen(out->failover_key);

            return out;
        }

        bool Replication::check_no_failover(const uint64_t& now, const Replication::QueueItem& item) {
            auto it = server.no_failover.find(item.failover_key);

            if(it != server.no_failover.end()) {
                if((it->second + server.no_failover_time) > now) {
                    return true; // It is ok to wait

                } else {
                    server.no_failover.erase(it);
                }
            }

            return false; // It is not ok to wait
        }

        bool Replication::failover(const uint64_t& now, const Utils::known_event_t& event) {
            auto& queue = *(event.r_queue);
            auto& queue_r2 = *(event.r2_queue);
            uint64_t item_len;
            auto _item = queue_r2.read(&item_len);

            if(_item == nullptr) {
                // fprintf(stderr, "replication: empty queue\n");
                return false;
            }

            auto& kv = *(queue_r2.kv);
            auto item = parse_queue_item(event, _item);
            auto cleanup = [&item, &_item](){
                free(item->peer_id);
                free(item->failover_key);
                delete item;
                free(_item);
            };

            bool key_removed = false;
            auto do_failover = [&kv, &queue, &queue_r2, &item, &_item, &item_len, &key_removed, &event](){
                auto commit = [&kv, &queue_r2, &event](){
                    if(kv.end_transaction(true)) {
                        return true;

                    } else {
                        fprintf(
                            stderr,
                            "Can't abort transaction for event %s: %s\n",
                            event.id,
                            kv.error().name()
                        );

                        return false;
                    }
                };

                if(kv.begin_transaction()) {
                    if(kv.cas(
                        item->failover_key,
                        item->failover_key_len,
                        "1", 1,
                        "1", 1
                    )) {
                        if(kv.remove(item->failover_key, item->failover_key_len)) {
                            queue.write(item_len, _item);

                            if(!commit()) {
                                return false;

                            } else {
                                key_removed = true;
                                return true;
                            }

                        } else {
                            fprintf(
                                stderr,
                                "Can't remove key %s of event %s: %s\n",
                                item->failover_key,
                                event.id,
                                kv.error().name()
                            );

                            commit();
                            return false;
                        }

                    } else if(!commit()) {
                        return false;
                    }

                } else {
                    fprintf(
                        stderr,
                        "Can't create transaction for event %s: %s\n",
                        event.id,
                        kv.error().name()
                    );

                    return false;
                }

                return true;
            };

            {
                auto it = server.failover.find(item->failover_key);

                if((it == server.failover.end()) && !do_failover()) {
                    queue_r2.sync_read_offset(false);
                    cleanup();
                    return false;
                }
            }

            if(check_no_failover(now, *item) || !do_failover()) {
                queue_r2.sync_read_offset(false);
                cleanup();
                return false;
            }

            if(!key_removed) {
                key_removed = kv.remove(item->failover_key, item->failover_key_len);

                if(!key_removed) {
                    key_removed = !kv.check(item->failover_key, item->failover_key_len);
                }
            }

            queue_r2.sync_read_offset(key_removed);
            cleanup();

            return true;
        }

        bool Replication::replication(const uint64_t& now, const Utils::known_event_t& event) {
            known_peers_t::const_iterator _peer;
            Skree::Client* peer;
            // fprintf(stderr, "replication: before read\n");
            auto& queue = *(event.r_queue);
            uint64_t item_len;
            auto _item = queue.read(&item_len);

            if(_item == nullptr) {
                // fprintf(stderr, "replication: empty queue\n");
                return false;
            }

            auto item = parse_queue_item(event, _item);
            auto cleanup = [&item, &_item](){
                free(item->peer_id);
                free(item->failover_key);
                delete item;
                free(_item);
            };

            // printf("repl thread: %s\n", event.id);

            // TODO: overflow
            if((item->rts + event.ttl) > now) {
                // fprintf(stderr, "skip repl: not now, rts: %llu, now: %llu\n", item->rts, now);
                cleanup();
                queue.sync_read_offset(false);
                return false;
            }

            {
                // fprintf(stderr, "asd1\n");
                auto it = server.failover.find(item->failover_key);
                // fprintf(stderr, "asd2\n");

                if(it != server.failover.end()) {
                    // TODO: what should really happen here?
                    // fprintf(stderr, "skip repl: failover flag is set\n");
                    cleanup();
                    queue.sync_read_offset(false);
                    return false;
                }
            }

            if(check_no_failover(now, *item)) {
                // TODO: what should really happen here?
                // fprintf(stderr, "skip repl: no_failover flag is set\n");
                cleanup();
                queue.sync_read_offset(false);
                return false;
            }

            server.failover[item->failover_key] = 0;
            server.no_failover[item->failover_key] = now;

            auto& queue_r2 = *(event.r2_queue);

            if(queue_r2.kv->add(item->failover_key, item->failover_key_len, "1", 1)) {
                queue_r2.write(item_len, _item);
                // fprintf(
                //     stderr,
                //     "Key %s for event %s has been added to r2_queue\n",
                //     item->failover_key,
                //     event.id
                // );

            } else {
                // fprintf(
                //     stderr,
                //     "Key %s for event %s already exists in r2_queue\n",
                //     item->failover_key,
                //     event.id
                // );
            }

            queue.sync_read_offset();
            // fprintf(stderr, "replication: after sync_read_offset(), rid: %llu\n", item->rid);

            pthread_mutex_lock(&(server.known_peers_mutex));

            _peer = server.known_peers.find(item->peer_id);

            if(_peer == server.known_peers.cend()) peer = nullptr;
            else peer = _peer->second;

            pthread_mutex_unlock(&(server.known_peers_mutex));

            // fprintf(stderr, "Seems like I need to failover task %llu\n", item->rid);

            if(peer == nullptr) {
                size_t offset = 0;
                bool have_rpr = false;

                uint32_t* count_replicas = (uint32_t*)malloc(sizeof(*count_replicas));
                uint32_t* acceptances = (uint32_t*)malloc(sizeof(*acceptances));
                uint32_t* pending = (uint32_t*)malloc(sizeof(*pending));

                *count_replicas = 0;
                *acceptances = 0;
                *pending = 0;

                pthread_mutex_t* mutex = (pthread_mutex_t*)malloc(sizeof(*mutex));
                pthread_mutex_init(mutex, nullptr);

                auto data_str = new Utils::muh_str_t {
                    .len = item->rin_len,
                    .data = item->rin
                };

                auto __peer_id = new Utils::muh_str_t {
                    .len = item->peer_id_len,
                    .data = item->peer_id
                };

                if(item->peers_cnt > 0) {
                    *count_replicas = item->peers_cnt;

                    auto i_req = Skree::Actions::I::out_init(__peer_id, event, item->rid_net);

                    size_t peer_id_len;
                    char* peer_id;
                    auto _peers_cnt = item->peers_cnt; // TODO

                    while(item->peers_cnt > 0) {
                        peer_id_len = strlen(item->rpr + offset); // TODO: get rid of this shit
                        peer_id = item->rpr + offset;
                        offset += peer_id_len + 1;

                        auto it = server.known_peers.find(peer_id);

                        if(it == server.known_peers.end()) {
                            pthread_mutex_lock(mutex);
                            ++(*acceptances);
                            pthread_mutex_unlock(mutex);

                        } else {
                            have_rpr = true;
                            pthread_mutex_lock(mutex);
                            ++(*pending);
                            pthread_mutex_unlock(mutex);

                            auto ctx = new out_packet_i_ctx {
                                .count_replicas = count_replicas,
                                .pending = pending,
                                .acceptances = acceptances,
                                .mutex = mutex,
                                .event = &event,
                                .data = data_str,
                                .peer_id = __peer_id,
                                .failover_key = item->failover_key,
                                .failover_key_len = item->failover_key_len,
                                .rpr = item->rpr, // TODO: why is it not nullptr here?
                                .peers_cnt = _peers_cnt, // TODO
                                .rid = item->rid
                            };

                            const auto cb = new Skree::PendingReads::Callbacks::ReplicationProposeSelf(server);
                            const auto item = new Skree::Base::PendingRead::QueueItem {
                                .len = 1,
                                .cb = cb,
                                .ctx = (void*)ctx,
                                .opcode = true,
                                .noop = false
                            };

                            auto witem = new Skree::Base::PendingWrite::QueueItem {
                                .len = i_req->len,
                                .data = i_req->data,
                                .pos = 0,
                                .cb = item
                            };

                            it->second->push_write_queue(witem);
                        }

                        --(item->peers_cnt);
                    }
                }

                if(!have_rpr) {
                    auto ctx = new out_packet_i_ctx {
                        .count_replicas = count_replicas,
                        .pending = pending,
                        .acceptances = acceptances,
                        .mutex = mutex,
                        .event = &event,
                        .data = data_str,
                        .peer_id = __peer_id,
                        .failover_key = item->failover_key,
                        .failover_key_len = item->failover_key_len,
                        .rpr = item->rpr, // TODO: why is it not nullptr here?
                        .peers_cnt = 0,
                        .rid = item->rid
                    };

                    server.replication_exec(ctx);
                }

            } else {
                // TODO: rin_str's type
                auto rin_str = new Utils::muh_str_t {
                    .len = item->rin_len,
                    .data = item->rin
                };

                Utils::muh_str_t* rpr_str = nullptr;

                if(item->peers_cnt > 0) {
                    rpr_str = new Utils::muh_str_t {
                        // .len = (uint32_t)strlen(item->rpr), // TODO: it is incorrect
                        .len = (uint32_t)(item_len - (item->rpr - _item)), // TODO: unreliable crutch
                        .data = item->rpr
                    };
                }

                auto ctx = new out_data_c_ctx {
                    .event = &event,
                    .rin = rin_str,
                    .rpr = rpr_str, // TODO?
                    .rid = item->rid,
                    .failover_key = item->failover_key,
                    .failover_key_len = item->failover_key_len
                };

                const auto cb = new Skree::PendingReads::Callbacks::ReplicationPingTask(server);
                const auto _item = new Skree::Base::PendingRead::QueueItem {
                    .len = 1,
                    .cb = cb,
                    .ctx = (void*)ctx,
                    .opcode = true,
                    .noop = false
                };

                auto c_req = Skree::Actions::C::out_init(event, item->rid_net, item->rin_len, item->rin);

                auto witem = new Skree::Base::PendingWrite::QueueItem {
                    .len = c_req->len,
                    .data = c_req->data,
                    .pos = 0,
                    .cb = _item
                };

                peer->push_write_queue(witem);
            }

            return true;
        }
    }
}
