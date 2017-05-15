#include "processor.hpp"
#include "../queue_db.hpp"
#include "../server.hpp"
#include "../actions/e.hpp"

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

        void Processor::Stat(Utils::known_event_t& event, uint32_t count) const {
            event.stat_num_processed += count;
        }

        bool Processor::do_failover(
            const uint64_t& now,
            Utils::known_event_t& event,
            uint64_t itemId,
            std::shared_ptr<Utils::muh_str_t> item,
            DbWrapper::TSession& kv_session,
            DbWrapper::TSession& queue_session
        ) const {
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
               kv_session,
               queue_session
            );

            if(result == SAVE_EVENT_RESULT_K) {
                auto itemIdNet = htonll(itemId);

                kv_session.remove((char*)&itemIdNet, sizeof(itemIdNet));
                queue_session.remove(itemId);

                return true; // success, key removed

            } else {
                return false; // failed, try again
            }
        }

        uint32_t Processor::process(const uint64_t& now, Utils::known_event_t& event) const {
            // Utils::cluck(1, "processor: before read\n");
            // auto& queue = *(event.queue);

            // const int id = event.group.ForkManager.WaitFreeWorker();

            auto queue2_session = event.queue2->kv->NewSession(DbWrapper::TSession::ST_QUEUE);

            // kv_session->begin_transaction();
            if(!queue2_session->begin_transaction()) {
                return 0;
            }

            auto kv_session = event.queue->kv->NewSession(DbWrapper::TSession::ST_KV);

            // if(!kv_session->begin_transaction()) { // this breaks cas()
            //     queue2_session->end_transaction(false); // this can fail too
            //     return 0;
            // }

            std::deque<std::pair<uint64_t, std::shared_ptr<Utils::muh_str_t>>> itemsToProcess;
            decltype(itemsToProcess) itemsToFailover;
            bool waited = false;
            auto& wip = server.wip;

            auto queue_session = event.queue->kv->NewSession(DbWrapper::TSession::ST_QUEUE);

            for(uint32_t i = 0; i < event.BatchSize; ++i) {
                uint64_t itemId;
                auto item = queue_session->next(itemId);

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

                        itemsToProcess.push_back(std::make_pair(itemId, item));

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

                        itemsToFailover.push_back(std::make_pair(itemId, item));
                    }

                } else if((i > 0) && !waited) {
                    waited = true;
                    // nanosleep(); // TODO
                    --i;
                    continue;

                } else {
                    break;
                }
            }

            // queue.Reset();

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
                for(const auto& item : itemsToFailover) {
                    do_failover(
                        now,
                        event,
                        item.first,
                        item.second,
                        *kv_session,
                        *queue_session
                    );
                }

                kv_session->Reset();
                queue_session->Reset();
            }

            if(itemsToProcess.empty()) {
                // Utils::cluck(1, "processor: empty queue\n");
                return itemsToFailover.size();
            }

            // TODO: process event here

            queue_session->begin_transaction(); // this can fail, but I think it is not critical

            for(const auto& item : itemsToProcess) {
                auto itemId = item.first;
                auto itemIdNet = htonll(itemId);

                if(!kv_session->remove((char*)&itemIdNet, sizeof(itemIdNet))) {
                    // TODO: what should really happen here?
                    // commit = (kv.check((char*)&itemIdNet, sizeof(itemIdNet)) <= 0);
                }

                if(/*commit && */!queue_session->remove(item.first /*itemId*/)) {
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

            queue_session->end_transaction(true);
            // Utils::cluck(2, "processor: after sync_read_offset(), rid: %llu\n", item->id);

            // if(commit) {
            //     // Utils::cluck(1, "[processor] ALL DONE");
            // }

            // TODO: better statistics
            return itemsToProcess.size() + itemsToFailover.size();//commit;
        }
    }
}
