#include "processor.hpp"
#include <ctime>
#include <limits>

namespace Skree {
    namespace Workers {
        void Processor::run() {
            uint64_t now;
            bool active;

            while(true) {
                now = std::time(nullptr);
                active = false;

                for(auto& it : server.known_events) {
                    auto& event = *(it.second);

                    // if(failover(now, event)) {
                    //     active = true;
                    //     ++(event.stat_num_failovered);
                    // }

                    uint32_t processedCount;
                    if((processedCount = process(now, event)) > 0) {
                        active = true;
                        event.stat_num_processed += processedCount;
                    }
                }

                if(!active) {
                    sleep(1);
                }
            }
        }

        bool Processor::do_failover(
            const uint64_t& now,
            Utils::known_event_t& event,
            uint64_t itemId,
            std::shared_ptr<Utils::muh_str_t> item
        ) {
            auto events = std::make_shared<std::vector<std::shared_ptr<in_packet_e_ctx_event>>>(1);

            (*events.get())[0].reset(new in_packet_e_ctx_event {
               .len = item->len,
               .data = item->data
            });

            in_packet_e_ctx e_ctx {
               .cnt = 1,
               .event_name_len = event.id_len,
               .event_name = event.id,
               .events = events,
               .origin = std::shared_ptr<void>(item, (void*)item.get()),
            };

            short result = server.save_event(
               e_ctx,
               0, // TODO: should wait for synchronous replication
               std::shared_ptr<Skree::Client>(),
               nullptr,
               *(event.queue)
            );

            if(result == SAVE_EVENT_RESULT_K) {
                auto itemIdNet = htonll(itemId);
                auto& kv = *(event.queue->kv);

                kv.remove((char*)&itemIdNet, sizeof(itemIdNet));
                kv.remove(itemId);

                return true; // success, key removed

            } else {
                return false; // failed, try again
            }
        }

        bool Processor::failover(const uint64_t& now, Utils::known_event_t& event) {
            auto& queue_r2 = *(event.queue2);
            uint64_t itemId;
            auto item = queue_r2.read(itemId);

            if(!item) {
                // Utils::cluck(1, "processor: empty queue\n");
                queue_r2.Reset();
                return false;
            }

            uint64_t origItemId;
            memcpy(&origItemId, item->data, sizeof(origItemId));
            origItemId = ntohll(origItemId);

            uint32_t itemLen = item->len - sizeof(origItemId);
            char* itemData = item->data + sizeof(origItemId);

            auto state = server.get_event_state(origItemId, event, now);
            bool repeat = false;
            bool key_removed = false;
            // short reason = 0;

            if(
                (state == SKREE_META_EVENTSTATE_PENDING)
                || (state == SKREE_META_EVENTSTATE_PROCESSING)
            ) {
                repeat = true;

            } else if(state == SKREE_META_EVENTSTATE_LOST) {
                std::shared_ptr<Utils::muh_str_t> _item;
                _item.reset(new Utils::muh_str_t {
                    .own = false,
                    .data = itemData,
                    .len = itemLen,
                    .origin = item,
                });

                if(do_failover(now, event, origItemId, _item)) {
                    key_removed = true;
                    // reason = 1;

                } else {
                    repeat = true;
                }
            }

            if(repeat) {
                Utils::cluck(3, "[processor::failover] releat: %llu, state: %u\n", origItemId, state);
                queue_r2.sync_read_offset(false);
                queue_r2.Reset();
                // cleanup();
                return false;
            }

            auto origItemIdNet = htonll(origItemId);

            if(!key_removed) {
                auto& kv = *(event.queue->kv);
                key_removed = kv.remove((char*)&origItemIdNet, sizeof(origItemIdNet));
                // reason = 2;

                if(!key_removed) {
                    key_removed = (kv.check((char*)&origItemIdNet, sizeof(origItemIdNet)) <= 0);
                    // reason = 3;
                }
            }

            if(key_removed) {
                auto& kv = *(event.queue->kv);
                key_removed = kv.remove(origItemId);
                // reason = 4;

                if(!key_removed) {
                    key_removed = (kv.check(origItemId) <= 0);
                    // reason = 5;
                }
            }

            if(key_removed) {
                auto& kv = *(event.queue2->kv);
                key_removed = kv.remove(itemId);
                // reason = 6;

                if(!key_removed) {
                    key_removed = (kv.check(itemId) <= 0);
                    // reason = 7;
                }
            }

            // if(key_removed) {
            //     Utils::cluck(3, "[processor::failover] key %llu removed, reason: %d\n", item->id, reason);
            // }

            queue_r2.sync_read_offset(key_removed);
            if(!key_removed) {
                queue_r2.Reset();
            }

            // if(key_removed) {
            //     event.queue2->kv->remove((char*)&itemIdNet, sizeof(itemIdNet));
            // }
            // cleanup();
            return key_removed;
        }

        uint32_t Processor::process(const uint64_t& now, Utils::known_event_t& event) {
            // Utils::cluck(1, "processor: before read\n");
            auto& queue = *(event.queue);
            std::deque<std::pair<uint64_t, std::shared_ptr<Utils::muh_str_t>>> items;
            bool waited = false;
            auto& wip = server.wip;

            auto kv_session = queue.kv->NewSession(DbWrapper::TSession::ST_KV);
            auto queue_session = queue.kv->NewSession(DbWrapper::TSession::ST_QUEUE);
            auto queue2_session = event.queue2->kv->NewSession(DbWrapper::TSession::ST_QUEUE);

            for(uint32_t i = 0; i < event.BatchSize; ++i) {
                uint64_t itemId;
                auto item = queue.read(itemId);

                if(item) {
                    if(server.get_event_state(itemId, event, now, kv_session.get())
                        == SKREE_META_EVENTSTATE_PROCESSING) {
                        break;
                    }

                    wip.lock();
                    wip[itemId] = now;
                    wip.unlock();

                    auto itemIdNet = htonll(itemId);

                    if(kv_session->cas((char*)&itemIdNet, sizeof(itemIdNet), "0", 1, "1", 1)) {
                        const size_t failover_item_len = sizeof(itemIdNet) + item->len;
                        char* failover_item = (char*)malloc(failover_item_len);

                        memcpy(failover_item, &itemIdNet, sizeof(itemIdNet));
                        memcpy(failover_item + sizeof(itemIdNet), item->data, item->len);

                        uint64_t key;
                        if(!queue2_session->append(&key, failover_item, failover_item_len)) {
                            abort();
                        }

                        free(failover_item);

                    } else {
                        // Utils::cluck(2, "db.cas() failed: %s\n", kv.error().name());
                        // size_t sz;
                        // char* val = kv.get((char*)&(item->id_net), sizeof(item->id_net), &sz);
                        // Utils::cluck(2, "value size: %lld\n", sz);
                        // if(sz > 0) {
                        //     char _val [sz + 1];
                        //     memcpy(_val, &val, sz);
                        //     _val[sz] = '\0';
                        //     Utils::cluck(2, "value: %s\n", _val);
                        // }
                        // abort();
                        do_failover(now, event, itemId, item);
                        break;
                    }

                    items.push_back(std::make_pair(itemId, item));

                } else if((i > 0) && !waited) {
                    waited = true;
                    // nanosleep(); // TODO
                    --i;
                    continue;

                } else {
                    break;
                }
            }

            if(items.empty()) {
                // Utils::cluck(1, "processor: empty queue\n");
                queue.Reset();
                return 0;
            }

            // TODO: process event here

            for(const auto& item : items) {
                auto itemId = item.first;
                auto itemIdNet = htonll(itemId);

                if(!kv_session->remove((char*)&itemIdNet, sizeof(itemIdNet))) {
                    // TODO: what should really happen here?
                    // commit = (kv.check((char*)&itemIdNet, sizeof(itemIdNet)) <= 0);
                }

                if(/*commit && */!queue_session->remove(itemId)) {
                    // TODO: what should really happen here?
                    // commit = (kv.check(itemId) <= 0);
                }

                auto wip_end = wip.lock();
                auto it = wip.find(itemId);

                if(it != wip_end) {
                    // TODO: this should not be done here unconditionally
                    wip.erase(it);
                }

                wip.unlock();
            }

            queue.Reset();
            // Utils::cluck(2, "processor: after sync_read_offset(), rid: %llu\n", item->id);

            // if(commit) {
            //     // Utils::cluck(1, "[processor] ALL DONE");
            // }

            return items.size();//commit;
        }
    }
}
