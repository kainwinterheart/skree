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

                if(active) {
                    ++(server.stat_num_repl_it);

                } else {
                    sleep(1);
                }
            }
        }

        std::shared_ptr<Replication::QueueItem> Replication::parse_queue_item(
            Utils::known_event_t& event,
            std::shared_ptr<Utils::muh_str_t> item
        ) {
            size_t item_pos = 0;
            std::shared_ptr<Replication::QueueItem> out;
            out.reset(new Replication::QueueItem {
                .rin = (item->data + item_pos + sizeof(uint32_t))
            });

            out->rin_len = ntohl(*(uint32_t*)(item->data + item_pos));
            item_pos += sizeof(out->rin_len);

            item_pos += out->rin_len;

            out->rts = ntohll(*(uint64_t*)(item->data + item_pos));
            item_pos += sizeof(out->rts);

            out->rid_net = *(uint64_t*)(item->data + item_pos);
            item_pos += sizeof(out->rid_net);

            out->rid = ntohll(out->rid_net);

            out->hostname_len = ntohl(*(uint32_t*)(item->data + item_pos));
            item_pos += sizeof(out->hostname_len);

            out->hostname = (char*)(item->data + item_pos); // TODO?
            item_pos += out->hostname_len;

            out->port = htonl(*(uint32_t*)(item->data + item_pos));
            item_pos += sizeof(out->port);

            out->peers_cnt = ntohl(*(uint32_t*)(item->data + item_pos));
            item_pos += sizeof(out->peers_cnt);

            out->rpr = (char*)(item->data + item_pos); // TODO?

            out->peer_id = Utils::make_peer_id(out->hostname_len, out->hostname, out->port);
            out->peer_id_len = strlen(out->peer_id);

            out->failover_key = (char*)malloc(
                out->peer_id_len
                + 1 // :
                + 20 // wrinseq
                + 1 // \0
            );
            sprintf(out->failover_key, "%s:%llu", out->peer_id, out->rid);
            // Utils::cluck(2, "repl thread: %s\n", suffix);

            out->failover_key_len = strlen(out->failover_key);
            out->origin = item;

            return out;
        }

        bool Replication::check_no_failover(
            const uint64_t& now,
            const Replication::QueueItem& item,
            Utils::known_event_t& event
        ) {
            auto& no_failover = event.no_failover;
            auto no_failover_end = no_failover.lock();
            auto it = no_failover.find(item.failover_key);

            if(it != no_failover_end) {
                if((it->second + server.no_failover_time) > now) {
                    no_failover.unlock();
                    return true; // It is ok to wait

                } else {
                    event.no_failover.erase(it);
                }
            }

            no_failover.unlock();

            return false; // It is not ok to wait
        }

        bool Replication::do_failover(
            const uint64_t& raw_item_len,
            char*& raw_item,
            const Replication::QueueItem& item,
            Utils::known_event_t& event
        ) {
            auto& queue = *(event.r_queue);
            auto& kv = *(queue.kv);

            auto commit = [&kv, &event](){
                if(kv.end_transaction(true)) {
                    return true;

                } else {
                    Utils::cluck(3,
                        "Can't commit transaction for event %s: %s\n",
                        event.id,
                        kv.error().name()
                    );

                    return false;
                }
            };

            if(kv.begin_transaction()) {
                if(kv.cas(item.failover_key, item.failover_key_len, "1", 1, "1", 1)) {
                    queue.write(raw_item_len, raw_item);

                    if(!kv.cas(item.failover_key, item.failover_key_len, "1", 1, "0", 1)) {
                        Utils::cluck(3,
                            "Can't remove key %s: %s\n",
                            item.failover_key,
                            kv.error().name()
                        );
                        // TODO: what should happen here?
                    }
                }

                return commit();

            } else {
                Utils::cluck(3,
                    "Can't create transaction for event %s: %s\n",
                    event.id,
                    kv.error().name()
                );

                return false;
            }
        }

        bool Replication::failover(const uint64_t& now, Utils::known_event_t& event) {
            auto& queue_r2 = *(event.r2_queue);
            auto _item = queue_r2.read();

            if(!_item) {
                // Utils::cluck(1, "replication: empty queue\n");
                return false;
            }

            auto item = parse_queue_item(event, _item);
            bool commit = true;

            {
                auto& failover = event.failover;
                auto failover_end = failover.lock();
                auto it = failover.find(item->failover_key);
                failover.unlock();

                if((it == failover_end) && !do_failover(_item->len, _item->data, *item, event)) {
                    commit = false;
                }
            }

            if(check_no_failover(now, *item, event) || !do_failover(_item->len, _item->data, *item, event)) {
                commit = false;
            }

            queue_r2.sync_read_offset(commit);

            free(item->peer_id);
            free(item->failover_key);
            // delete item;
            // free(_item);

            return commit;
        }

        bool Replication::replication(const uint64_t& now, Utils::known_event_t& event) {
            // Utils::cluck(1, "replication: before read\n");
            auto& queue = *(event.r_queue);
            auto _item = queue.read();

            if(!_item) {
                // Utils::cluck(1, "replication: empty queue\n");
                return false;
            }

            auto item = parse_queue_item(event, _item);
            auto cleanup = [&item](){
                free(item->peer_id);
                free(item->failover_key);
                // delete item;
                // free(_item);
            };

            // Utils::cluck(2, "repl thread: %s\n", event.id);

            // TODO: overflow
            if((item->rts + event.ttl) > now) {
                // Utils::cluck(3, "skip repl: not now, rts: %llu, now: %llu\n", item->rts, now);
                cleanup();
                queue.sync_read_offset(false);
                return false;
            }

            auto& failover = event.failover;

            {
                // Utils::cluck(1, "asd1\n");
                auto failover_end = failover.lock();
                auto it = failover.find(item->failover_key);
                failover.unlock();
                // Utils::cluck(1, "asd2\n");

                if(it != failover_end) {
                    // TODO: what should really happen here?
                    // Utils::cluck(1, "skip repl: failover flag is set\n");
                    cleanup();
                    queue.sync_read_offset(false);
                    return false;
                }
            }

            if(check_no_failover(now, *item, event)) {
                // TODO: what should really happen here?
                // Utils::cluck(1, "skip repl: no_failover flag is set\n");
                cleanup();
                queue.sync_read_offset(false);
                return false;
            }

            failover.lock();
            failover[item->failover_key] = 0;
            failover.unlock();

            auto& no_failover = event.no_failover;
            no_failover.lock();
            no_failover[item->failover_key] = now;
            no_failover.unlock();

            bool commit = true;

            if(queue.kv->cas(item->failover_key, item->failover_key_len, "0", 1, "1", 1)) {
                event.r2_queue->write(_item->len, _item->data);
                // Utils::cluck(3,
                //     "Key %s for event %s has been added to r2_queue\n",
                //     item->failover_key,
                //     event.id
                // );

            } else if(queue.kv->check(item->failover_key, item->failover_key_len) > 0) {
                Utils::cluck(3,
                    "Key %s could not be added to r2_queue: %s\n",
                    item->failover_key,
                    queue.kv->error().name()
                );
                commit = false;
                do_failover(_item->len, _item->data, *item, event);
            }

            queue.sync_read_offset(commit);

            if(!commit) {
                event.unfailover(item->failover_key);
                cleanup();
                return false;
            }
            // Utils::cluck(2, "replication: after sync_read_offset(), rid: %llu\n", item->rid);

            auto& known_peers = server.known_peers;
            auto known_peers_end = known_peers.lock();
            auto _peer = known_peers.find(item->peer_id);
            known_peers.unlock();

            Skree::Client* peer = ((_peer == known_peers_end) ? nullptr : _peer->second.next());

            // Utils::cluck(2, "Seems like I need to failover task %llu\n", item->rid);

            if(peer == nullptr) {
                size_t offset = 0;
                bool have_rpr = false;

                auto count_replicas = std::make_shared<uint32_t>(0);
                auto acceptances = std::make_shared<uint32_t>(0);
                auto pending = std::make_shared<uint32_t>(0);

                auto mutex = std::make_shared<pthread_mutex_t>();
                pthread_mutex_init(mutex.get(), nullptr);

                std::shared_ptr<Utils::muh_str_t> data_str;
                data_str.reset(new Utils::muh_str_t {
                    .own = false,
                    .len = item->rin_len,
                    .data = (char*)(item->rin) // TODO
                });

                std::shared_ptr<Utils::muh_str_t> __peer_id;
                __peer_id.reset(new Utils::muh_str_t {
                    .own = false,
                    .len = item->peer_id_len,
                    .data = item->peer_id
                });

                if(item->peers_cnt > 0) {
                    *count_replicas = item->peers_cnt;

                    auto _peers_cnt = item->peers_cnt; // TODO
                    // Utils::cluck(2, "asd: %u, %lu, %lu", item->peers_cnt, item->rid, offset);
                    while(item->peers_cnt > 0) {
                        // Utils::cluck(2, "qwe: %u", item->peers_cnt);
                        size_t peer_id_len = strlen(item->rpr + offset); // TODO: get rid of this shit
                        // Utils::cluck(2, "eqw: %u", item->peers_cnt);
                        const char* peer_id = item->rpr + offset;
                        offset += peer_id_len + 1;

                        known_peers_end = known_peers.lock();
                        auto it = known_peers.find(peer_id);
                        known_peers.unlock();

                        if(it == known_peers_end) {
                            pthread_mutex_lock(mutex.get());
                            ++(*acceptances);
                            pthread_mutex_unlock(mutex.get());

                        } else {
                            have_rpr = true;
                            pthread_mutex_lock(mutex.get());
                            ++(*pending);
                            pthread_mutex_unlock(mutex.get());

                            std::shared_ptr<out_packet_i_ctx> ctx;
                            ctx.reset(new out_packet_i_ctx {
                                .count_replicas = count_replicas,
                                .pending = pending,
                                .acceptances = acceptances,
                                .mutex = mutex,
                                .event = &event,
                                .data = data_str,
                                .peer_id = __peer_id,
                                .failover_key = item->failover_key,
                                .failover_key_len = item->failover_key_len,
                                .rpr = item->rpr,
                                .peers_cnt = _peers_cnt, // TODO
                                .rid = item->rid,
                                .origin = _item
                            });

                            auto i_req = Skree::Actions::I::out_init(__peer_id, event, item->rid_net);
                            std::shared_ptr<void> _ctx (ctx, (void*)ctx.get());

                            i_req->set_cb(std::make_shared<Skree::Base::PendingRead::QueueItem>(
                                _ctx, std::make_shared<Skree::PendingReads::Callbacks::ReplicationProposeSelf>(server)
                            ));

                            it->second.next()->push_write_queue(i_req);
                        }

                        --(item->peers_cnt);
                    }
                    // Utils::cluck(2, "zxc: %u", item->peers_cnt);
                }

                if(!have_rpr) {
                    out_packet_i_ctx ctx {
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
                        .rid = item->rid,
                        .origin = _item
                    };

                    server.replication_exec(ctx);
                }

            } else {
                // TODO: rin_str's type
                std::shared_ptr<Utils::muh_str_t> rin_str;
                rin_str.reset(new Utils::muh_str_t {
                    .own = false,
                    .len = item->rin_len,
                    .data = (char*)(item->rin) // TODO?
                });

                std::shared_ptr<Utils::muh_str_t> rpr_str;

                if(item->peers_cnt > 0) {
                    rpr_str.reset(new Utils::muh_str_t {
                        .own = false,
                        // .len = (uint32_t)strlen(item->rpr), // TODO: it is incorrect
                        .len = (uint32_t)(_item->len - (item->rpr - _item->data)), // TODO: unreliable crutch
                        .data = item->rpr
                    });
                }

                std::shared_ptr<out_data_c_ctx> ctx;
                ctx.reset(new out_data_c_ctx {
                    .event = &event,
                    .rin = rin_str,
                    .rpr = rpr_str, // TODO?
                    .rid = item->rid,
                    .failover_key = item->failover_key,
                    .failover_key_len = item->failover_key_len,
                    .origin = _item
                });

                auto c_req = Skree::Actions::C::out_init(event, item->rid_net, item->rin_len, item->rin);
                std::shared_ptr<void> _ctx (ctx, (void*)ctx.get());

                c_req->set_cb(std::make_shared<Skree::Base::PendingRead::QueueItem>(
                    _ctx, std::make_shared<Skree::PendingReads::Callbacks::ReplicationPingTask>(server)
                ));

                c_req->finish();

                peer->push_write_queue(c_req);
            }

            return commit;
        }
    }
}
