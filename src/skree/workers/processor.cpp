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
                    if(failover(now, *(it.second))) {
                        active = true;
                    }

                    if(process(now, *(it.second))) {
                        active = true;
                    }
                }

                ++(server.stat_num_proc_it);

                if(!active) {
                    sleep(1);
                }
            }
        }

        Processor::QueueItem* Processor::parse_queue_item(
            const Utils::known_event_t& event,
            const uint64_t& item_len,
            char*& item
        ) {
            auto out = new Processor::QueueItem;

            memcpy(&(out->id_net), item, sizeof(out->id_net));

            out->id = ntohll(out->id_net);
            out->data = item + sizeof(out->id);
            out->len = item_len - sizeof(out->id);

            return out;
        }

        bool Processor::do_failover(
            const uint64_t& now,
            const Utils::known_event_t& event,
            const Processor::QueueItem& item
        ) {
            in_packet_e_ctx_event _event {
               .len = item.len,
               .data = item.data
            };

            in_packet_e_ctx_event* events [1];
            events[0] = &_event;

            in_packet_e_ctx e_ctx {
               .cnt = 1,
               .event_name_len = event.id_len,
               .event_name = event.id,
               .events = events
            };

            short result = server.save_event(
               &e_ctx,
               0, // TODO: should wait for synchronous replication
               nullptr,
               nullptr,
               *(event.queue)
            );

            if(result == SAVE_EVENT_RESULT_K) {
                auto& queue_r2 = *(event.queue2);
                auto& kv = *(queue_r2.kv);
                kv.remove((char*)&(item.id_net), sizeof(item.id_net));

                return true; // success, key removed

            } else {
                return false; // failed, try again
            }
        }

        bool Processor::failover(const uint64_t& now, const Utils::known_event_t& event) {
            auto& queue_r2 = *(event.queue2);
            uint64_t item_len;
            auto _item = queue_r2.read(&item_len);

            if(_item == nullptr) {
                // fprintf(stderr, "processor: empty queue\n");
                return false;
            }

            auto item = parse_queue_item(event, item_len, _item);
            auto cleanup = [&item, &_item](){
                delete item;
                free(_item);
            };

            auto state = server.get_event_state(item->id, event, now);
            bool repeat = false;
            bool key_removed = false;

            if(
                (state == SKREE_META_EVENTSTATE_PENDING)
                || (state == SKREE_META_EVENTSTATE_PROCESSING)
            ) {
                repeat = true;

            } else if(state == SKREE_META_EVENTSTATE_LOST) {
                if(do_failover(now, event, *item)) {
                    key_removed = true;

                } else {
                    repeat = true;
                }
            }

            if(repeat) {
                queue_r2.sync_read_offset(false);
                cleanup();
                return false;
            }

            if(!key_removed) {
                auto& kv = *(queue_r2.kv);

                key_removed = kv.remove((char*)&(item->id_net), sizeof(item->id_net));

                if(!key_removed) {
                    key_removed = (kv.check((char*)&(item->id_net), sizeof(item->id_net)) <= 0);
                }
            }

            queue_r2.sync_read_offset(key_removed);
            cleanup();
            return true;
        }

        bool Processor::process(const uint64_t& now, const Utils::known_event_t& event) {
            known_peers_t::const_iterator _peer;
            Skree::Client* peer;
            // fprintf(stderr, "processor: before read\n");
            auto& queue = *(event.queue);
            uint64_t item_len;
            auto _item = queue.read(&item_len);

            if(_item == nullptr) {
                // fprintf(stderr, "processor: empty queue\n");
                return false;
            }

            // TODO: batch event processing

            auto item = parse_queue_item(event, item_len, _item);
            auto cleanup = [&item, &_item](){
                delete item;
                free(_item);
            };

            if(server.get_event_state(item->id, event, now) == SKREE_META_EVENTSTATE_PROCESSING) {
                // TODO: what should really happen here?
                // fprintf(stderr, "skip repl: no_failover flag is set\n");
                cleanup();
                queue.sync_read_offset(false);
                return false;
            }

            server.wip[item->id] = now;

            auto& queue_r2 = *(event.queue2);
            bool commit = true;

            if(queue_r2.kv->cas((char*)&(item->id_net), sizeof(item->id_net), "0", 1, "1", 1)) {
                queue_r2.write(item_len, _item);

                // TODO: process event here

            } else if(!do_failover(now, event, *item)) {
                commit = false;
            }

            queue.sync_read_offset(commit);
            // fprintf(stderr, "processor: after sync_read_offset(), rid: %llu\n", item->id);

            return true;
        }
    }
}
