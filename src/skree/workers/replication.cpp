#include "replication.hpp"
#include <ctime>

namespace Skree {
    namespace Workers {
        void Replication::run() {
            uint64_t now;
            bool active;

            while(true) {
                now = std::time(nullptr);
                active = false;

                for(auto& it : server.known_events) {
                    auto& event = *(it.second);
                    uint32_t processedCount;

                    if((processedCount = process(now, event)) > 0) {
                        active = true;
                        Stat(event, processedCount);
                    }
                }

                if(!active) {
                    sleep(1);
                }
            }
        }

        void Replication::Stat(Utils::known_event_t& event, uint32_t count) const {
            server.stat_num_repl_it += count;
        }

        std::shared_ptr<Replication::QueueItem> Replication::parse_queue_item(
            Utils::known_event_t& event,
            // const uint64_t itemId,
            std::shared_ptr<Utils::muh_str_t> item
        ) {
            size_t item_pos = 0;
            std::shared_ptr<Replication::QueueItem> out;
            out.reset(new Replication::QueueItem {
                .rin = (item->data + item_pos + sizeof(uint32_t))
            });

            memcpy(&(out->rin_len), (item->data + item_pos), sizeof(uint32_t));
            item_pos += sizeof(out->rin_len);
            out->rin_len = ntohl(out->rin_len);

            item_pos += out->rin_len;

            memcpy(&(out->rts), (item->data + item_pos), sizeof(uint64_t));
            item_pos += sizeof(out->rts);
            out->rts = ntohll(out->rts);

            memcpy(&(out->rid_net), (item->data + item_pos), sizeof(uint64_t));
            item_pos += sizeof(out->rid_net);

            out->rid = ntohll(out->rid_net);
            // out->rid_net = htonll(itemId);
            // out->rid = itemId; // THIS IS WRONG, should be remote id

            memcpy(&(out->hostname_len), (item->data + item_pos), sizeof(uint32_t));
            item_pos += sizeof(out->hostname_len);
            out->hostname_len = ntohl(out->hostname_len);

            out->hostname = item->data + item_pos;
            item_pos += out->hostname_len;

            memcpy(&(out->port), (item->data + item_pos), sizeof(uint32_t));
            item_pos += sizeof(out->port);
            out->port = htonl(out->port);

            memcpy(&(out->peers_cnt), (item->data + item_pos), sizeof(uint32_t));
            item_pos += sizeof(out->peers_cnt);
            out->peers_cnt = ntohl(out->peers_cnt);

            out->rpr = item->data + item_pos;

            out->peer_id = Utils::make_peer_id(out->hostname_len, out->hostname, out->port);
            out->failover_key = Utils::NewStr(
                out->peer_id->len
                + 1 // :
                + 20 // wrinseq
                + 1 // \0
            );
            sprintf(out->failover_key->data, "%s:%llu", out->peer_id->data, out->rid);
            out->failover_key->len = strlen(out->failover_key->data);
            // Utils::cluck(2, "repl thread: %s\n", suffix);

            out->origin = item;

            return out;
        }

        bool Replication::check_no_failover(
            const uint64_t& now,
            const Replication::QueueItem& item,
            Utils::known_event_t& event
        ) const {
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
            Utils::known_event_t& event,
            DbWrapper::TSession& kvSession,
            DbWrapper::TSession& queueSession
        ) const {
            // auto& queue = *(event.r_queue);
            // auto kv = queue.kv->NewSession();

            auto commit = [&kvSession, &event](){
                if(kvSession.end_transaction(true)) {
                    return true;

                } else {
                    Utils::cluck(2,
                        "Can't commit transaction for event %s",
                        event.id
                        // kv->error().name()
                    );

                    return false;
                }
            };

            if(kvSession.begin_transaction()) {
                const auto& status = kvSession.get(item.failover_key->data, item.failover_key->len);

                // TODO: review all conditions that need to be failovered
                if((status.size() == (1 + sizeof(uint64_t))) && (status[0] == '1')) {
                    uint64_t key;
                    queueSession.append(&key, raw_item, raw_item_len);

                    key = htonll(key);

                    char failoverValue [1 + sizeof(key)];

                    failoverValue[0] = '0';
                    memcpy(failoverValue + 1, &key, sizeof(key));

                    if(!kvSession.set(
                        item.failover_key->data, item.failover_key->len,
                        failoverValue, 1 + sizeof(key)
                    )) {
                        Utils::cluck(2,
                            "Can't update key %s",
                            item.failover_key->data
                            // kv->error().name()
                        );
                        // TODO: what should happen here?
                    }
                }

                return commit();

            } else {
                Utils::cluck(2,
                    "Can't create transaction for event %s",
                    event.id
                    // kv->error().name()
                );

                return false;
            }
        }

        uint32_t Replication::process(const uint64_t& now, Utils::known_event_t& event) const {
            // Utils::cluck(1, "replication: before read\n");
            auto queue2_session = event.r2_queue->kv->NewSession(DbWrapper::TSession::ST_QUEUE);

            if(!queue2_session->begin_transaction()) {
                return 0;
            }

            auto kv_session = event.r_queue->kv->NewSession(DbWrapper::TSession::ST_KV);

            // if(!kv_session->begin_transaction()) { // this breaks cas()
            //     queue2_session->end_transaction(false); // this can fail too
            //     return 0;
            // }

            std::deque<std::tuple<
                uint64_t,
                std::shared_ptr<Utils::muh_str_t>,
                std::shared_ptr<QueueItem>
            >> itemsToProcess;
            decltype(itemsToProcess) itemsToFailover;

            bool waited = false;
            auto& failover = event.failover;

            auto queue_session = event.r_queue->kv->NewSession(DbWrapper::TSession::ST_QUEUE);

            for(uint32_t i = 0; i < event.BatchSize; ++i) {
                uint64_t itemId;
                auto _item = queue_session->next(itemId);

                if(_item) {
                    auto item = parse_queue_item(event,/* itemId,*/ _item);

                    // Utils::cluck(2, "repl thread: %s\n", event.id);

                    // TODO: overflow
                    if((item->rts + event.ttl) > now) {
                        // Utils::cluck(3, "skip repl: not now, rts: %llu, now: %llu\n", item->rts, now);
                        // cleanup();
                        break;
                    }

                    {
                        // Utils::cluck(1, "asd1\n");
                        auto failover_end = failover.lock();
                        auto it = failover.find(item->failover_key);
                        failover.unlock();
                        // Utils::cluck(1, "asd2\n");

                        if(it != failover_end) {
                            // TODO: what should really happen here?
                            // Utils::cluck(1, "skip repl: failover flag is set\n");
                            // cleanup();
                            break;
                        }
                    }

                    if(check_no_failover(now, *item, event)) {
                        // TODO: what should really happen here?
                        // Utils::cluck(1, "skip repl: no_failover flag is set\n");
                        // cleanup();
                        break;
                    }

                    failover.lock();
                    failover[item->failover_key] = 0;
                    failover.unlock();

                    auto& no_failover = event.no_failover;
                    no_failover.lock();
                    no_failover[item->failover_key] = now;
                    no_failover.unlock();

                    const auto itemIdNet = htonll(itemId);
                    char oldFailoverValue [1 + sizeof(itemIdNet)];
                    char newFailoverValue [1 + sizeof(itemIdNet)];

                    oldFailoverValue[0] = '0';
                    newFailoverValue[0] = '1';
                    memcpy(oldFailoverValue + 1, &itemIdNet, sizeof(itemIdNet));
                    memcpy(newFailoverValue + 1, &itemIdNet, sizeof(itemIdNet));

                    if(kv_session->cas(
                        item->failover_key->data, item->failover_key->len,
                        oldFailoverValue, 1 + sizeof(itemIdNet),
                        newFailoverValue, 1 + sizeof(itemIdNet)
                    )) {
                        uint64_t key;
                        queue2_session->append(&key, _item->data, _item->len);
                        // Utils::cluck(3,
                        //     "Key %s for event %s has been added to r2_queue\n",
                        //     item->failover_key,
                        //     event.id
                        // );
                        itemsToProcess.push_back(std::make_tuple(itemId, _item, item));

                    } else if(kv_session->check(item->failover_key->data, item->failover_key->len) > 0) {
                        Utils::cluck(2,
                            "Key %s could not be added to r2_queue",
                            item->failover_key->data
                            // queue.kv->error().name()
                        );
                        itemsToFailover.push_back(std::make_tuple(itemId, _item, item));
                        // commit = false;
                    }

                } else if((i > 0) && !waited) { // TODO: is this really necessary here?
                    waited = true;
                    // nanosleep(); // TODO
                    --i;
                    continue;

                } else {
                    break;
                }
            }

            {
                const auto queue2_rv = queue2_session->end_transaction(true);
                // kv_session->end_transaction(queue2_rv);

                if(!queue2_rv) {
                    return 0;
                }
            }

            kv_session->Reset();
            queue_session->Reset();
            queue2_session->Reset();

            if(!itemsToFailover.empty()) {
                for(const auto& node : itemsToFailover) {
                    const auto& [itemId, _item, item] = node;

                    if(!do_failover(
                        _item->len,
                        _item->data,
                        *item,
                        event,
                        *kv_session,
                        *queue_session
                    )) {
                        event.unfailover(item->failover_key);
                    }
                }

                kv_session->Reset();
                queue_session->Reset();
            }

            if(itemsToProcess.empty()) {
                // Utils::cluck(1, "replication: empty queue\n");
                return itemsToFailover.size();
            }

            auto& known_peers = server.known_peers;

            for(const auto& item : itemsToProcess) {
                if(!queue_session->remove(std::get<0>(item))) {
                    // TODO: what should really happen here?
                    // commit = (queue.kv->check(itemId) <= 0);
                }
            }

            queue_session->Reset();

            for(const auto& node : itemsToProcess) {
                // Utils::cluck(2, "replication: after sync_read_offset(), rid: %llu\n", item->rid);
                const auto& [itemId, _item, item] = node;

                auto known_peers_end = known_peers.lock();
                auto _peer = known_peers.find(item->peer_id);
                known_peers.unlock();

                Skree::Client* peer = ((_peer == known_peers_end) ? nullptr : _peer->second.next());

                // Utils::cluck(2, "Seems like I need to failover task %llu\n", item->rid);

                if(peer == nullptr) {
                    bool should_run_replication_exec = true;

                    auto count_replicas = std::make_shared<uint32_t>(0);
                    auto acceptances = std::make_shared<uint32_t>(0);
                    auto rejects = std::make_shared<uint32_t>(0);

                    auto mutex = std::make_shared<Utils::TSpinLock>();

                    std::shared_ptr<Utils::muh_str_t> data_str(
                        new Utils::muh_str_t((char*)(item->rin), item->rin_len, false) // TODO: review item->rin usage
                    );

                    std::shared_ptr<std::deque<std::shared_ptr<Utils::muh_str_t>>> peers;

                    // Utils::cluck(2, "item->peers_cnt = %lu", item->peers_cnt);

                    if(item->peers_cnt > 0) {
                        *count_replicas = item->peers_cnt;
                        size_t offset = 0;

                        peers.reset(new std::deque<std::shared_ptr<Utils::muh_str_t>>());

                        while(item->peers_cnt > 0) {
                            uint32_t peer_name_len;
                            memcpy(&peer_name_len, (item->rpr + offset), sizeof(peer_name_len));
                            offset += sizeof(peer_name_len);
                            peer_name_len = ntohl(peer_name_len);

                            const char* peer_name = item->rpr + offset;
                            offset += peer_name_len;

                            uint32_t peer_port;
                            memcpy(&peer_port, (item->rpr + offset), sizeof(peer_port));
                            offset += sizeof(peer_port);
                            peer_port = ntohl(peer_port);

                            peers->push_back(Utils::make_peer_id(
                                peer_name_len,
                                peer_name,
                                peer_port
                            ));

                            --(item->peers_cnt);
                        }

                        for(const auto& _peer_id : *peers) {
                            ++(item->peers_cnt);

                            known_peers_end = known_peers.lock();
                            auto it = known_peers.find(_peer_id);
                            known_peers.unlock();

                            if(it == known_peers_end) {
                                // Utils::cluck(2, "Peer %s NOT found", _peer_id->data);
                                Utils::TSpinLockGuard guard(*mutex);
                                ++(*acceptances);

                                if((*acceptances + *rejects) == *count_replicas) {
                                    should_run_replication_exec = true;
                                }

                            } else {
                                // Utils::cluck(2, "Peer %s is found", _peer_id->data);
                                should_run_replication_exec = false;

                                std::shared_ptr<out_packet_i_ctx> ctx;
                                ctx.reset(new out_packet_i_ctx {
                                    .count_replicas = count_replicas,
                                    .acceptances = acceptances,
                                    .rejects = rejects,
                                    .mutex = mutex,
                                    .event = &event,
                                    .data = data_str,
                                    .peer_id = item->peer_id,
                                    .failover_key = item->failover_key,
                                    .rpr = peers,
                                    .rid = item->rid,
                                    .origin = _item
                                });

                                auto i_req = Skree::Actions::I::out_init(item->peer_id, event, item->rid_net);

                                i_req->set_cb(std::make_shared<Skree::Base::PendingRead::QueueItem>(
                                    std::shared_ptr<void>(ctx, (void*)ctx.get()),
                                    std::make_shared<Skree::PendingReads::Callbacks::ReplicationProposeSelf>(server)
                                ));

                                it->second.next()->push_write_queue(i_req);
                            }
                        }
                        // Utils::cluck(2, "zxc: %u", item->peers_cnt);
                    }

                    if(should_run_replication_exec) {
                        std::shared_ptr<out_packet_i_ctx> ctx;
                        ctx.reset(new out_packet_i_ctx {
                            .count_replicas = count_replicas,
                            .acceptances = acceptances,
                            .rejects = rejects,
                            .mutex = mutex,
                            .event = &event,
                            .data = data_str,
                            .peer_id = item->peer_id,
                            .failover_key = item->failover_key,
                            .rpr = peers,
                            .rid = item->rid,
                            .origin = _item
                        });

                        server.replication_exec(ctx);
                    }

                } else {
                    // TODO: rin_str's type
                    std::shared_ptr<Utils::muh_str_t> rin_str(
                        new Utils::muh_str_t((char*)(item->rin), item->rin_len, false) // TODO?: review item->rin_len usage
                    );

                    std::shared_ptr<out_data_c_ctx> ctx;
                    ctx.reset(new out_data_c_ctx {
                        .event = &event,
                        .rin = rin_str,
                        .rid = item->rid,
                        .failover_key = item->failover_key,
                        .origin = _item
                    });

                    auto c_req = Skree::Actions::C::out_init(event, item->rid_net, item->rin_len, item->rin);

                    c_req->set_cb(std::make_shared<Skree::Base::PendingRead::QueueItem>(
                        std::shared_ptr<void>(ctx, (void*)ctx.get()),
                        std::make_shared<Skree::PendingReads::Callbacks::ReplicationPingTask>(server)
                    ));

                    c_req->finish();

                    peer->push_write_queue(c_req);
                }
            }

            return itemsToProcess.size() + itemsToFailover.size();
        }
    }
}
