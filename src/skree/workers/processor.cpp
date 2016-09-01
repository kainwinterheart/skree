#include "processor.hpp"

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

                    if(failover(now, event)) {
                        active = true;
                        ++(event.stat_num_processed);
                    }

                    if(process(now, event)) {
                        active = true;
                        ++(event.stat_num_failovered);
                    }
                }

                if(!active) {
                    sleep(1);
                }
            }
        }

        std::shared_ptr<Processor::QueueItem> Processor::parse_queue_item(
            Utils::known_event_t& event,
            const uint64_t item_len,
            const char* item
        ) {
            std::shared_ptr<Processor::QueueItem> _item;
            _item.reset(new Processor::QueueItem {
                .id_net = *(uint64_t*)item,
                .id = ntohll(*(uint64_t*)item),
                .data = item + sizeof(uint64_t),
                .len = item_len - sizeof(uint64_t)
            });

            return _item;
        }

        bool Processor::do_failover(
            const uint64_t& now,
            Utils::known_event_t& event,
            const Processor::QueueItem& item
        ) {
            auto events = std::make_shared<std::vector<std::shared_ptr<in_packet_e_ctx_event>>>(1);

            (*events.get())[0].reset(new in_packet_e_ctx_event {
               .len = (uint32_t)(item.len), // TODO
               .data = item.data
            });

            in_packet_e_ctx e_ctx {
               .cnt = 1,
               .event_name_len = event.id_len,
               .event_name = event.id,
               .events = events
            };

            short result = server.save_event(
               e_ctx,
               0, // TODO: should wait for synchronous replication
               nullptr,
               nullptr,
               *(event.queue)
            );

            if(result == SAVE_EVENT_RESULT_K) {
                auto& kv = *(event.queue->kv);
                kv.remove((char*)&(item.id_net), sizeof(item.id_net));

                return true; // success, key removed

            } else {
                return false; // failed, try again
            }
        }

        bool Processor::failover(const uint64_t& now, Utils::known_event_t& event) {
            auto& queue_r2 = *(event.queue2);
            uint64_t item_len;
            auto _item = queue_r2.read(&item_len);

            if(_item == nullptr) {
                // Utils::cluck(1, "processor: empty queue\n");
                return false;
            }

            auto item = parse_queue_item(event, item_len, _item);
            auto cleanup = [&item, &_item](){
                // delete item;
                free(_item);
            };

            auto state = server.get_event_state(item->id, event, now);
            bool repeat = false;
            bool key_removed = false;
            // short reason = 0;

            if(
                (state == SKREE_META_EVENTSTATE_PENDING)
                || (state == SKREE_META_EVENTSTATE_PROCESSING)
            ) {
                repeat = true;

            } else if(state == SKREE_META_EVENTSTATE_LOST) {
                if(do_failover(now, event, *item)) {
                    key_removed = true;
                    // reason = 1;

                } else {
                    repeat = true;
                }
            }

            if(repeat) {
                Utils::cluck(3, "[processor::failover] releat: %llu, state: %u\n", item->id, state);
                queue_r2.sync_read_offset(false);
                cleanup();
                return false;
            }

            if(!key_removed) {
                auto& kv = *(event.queue->kv);
                key_removed = kv.remove((char*)&(item->id_net), sizeof(item->id_net));
                // reason = 2;

                if(!key_removed) {
                    key_removed = (kv.check((char*)&(item->id_net), sizeof(item->id_net)) <= 0);
                    // reason = 3;
                }
            }

            // if(key_removed) {
            //     Utils::cluck(3, "[processor::failover] key %llu removed, reason: %d\n", item->id, reason);
            // }

            queue_r2.sync_read_offset(key_removed);
            cleanup();
            return key_removed;
        }

        bool Processor::process(const uint64_t& now, Utils::known_event_t& event) {
            Skree::Client* peer;
            // Utils::cluck(1, "processor: before read\n");
            auto& queue = *(event.queue);
            uint64_t item_len;
            auto _item = queue.read(&item_len);

            if(_item == nullptr) {
                // Utils::cluck(1, "processor: empty queue\n");
                return false;
            }

            // TODO: batch event processing

            auto item = parse_queue_item(event, item_len, _item);
            auto cleanup = [&item, &_item](){
                // delete item;
                free(_item);
            };

            if(server.get_event_state(item->id, event, now) == SKREE_META_EVENTSTATE_PROCESSING) {
                // TODO: what should really happen here?
                // Utils::cluck(1, "skip repl: no_failover flag is set\n");
                cleanup();
                queue.sync_read_offset(false);
                return false;
            }

            auto& wip = server.wip;
            wip.lock();
            wip[item->id] = now;
            wip.unlock();

            bool commit = true;
            auto& kv = *(queue.kv);

            if(kv.cas((char*)&(item->id_net), sizeof(item->id_net), "0", 1, "1", 1)) {
                event.queue2->write(item_len, _item);

                // TODO: process event here

                if(!kv.remove((char*)&(item->id_net), sizeof(item->id_net))) {
                    // TODO: what should really happen here?
                    commit = (kv.check((char*)&(item->id_net), sizeof(item->id_net)) <= 0);
                }

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
                if(!do_failover(now, event, *item)) {
                commit = false;
            }}

            queue.sync_read_offset(commit);
            // Utils::cluck(2, "processor: after sync_read_offset(), rid: %llu\n", item->id);

            auto wip_end = wip.lock();
            auto it = wip.find(item->id);

            if(it != wip_end) {
                // TODO: this should not be done here unconditionally
                wip.erase(it);
            }

            wip.unlock();

            return commit;
        }
    }
}
