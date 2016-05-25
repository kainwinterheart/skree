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

        bool Processor::check_wip(const uint64_t& now, const Processor::QueueItem& item) {
            auto it = server.wip.find(item.id);

            if(it != server.wip.end()) {
                // TODO: check for overflow
                if((it->second + server.job_time) > now) {
                    return true; // It is ok to wait

                } else {
                    server.wip.erase(it);
                }
            }

            return false; // It is not ok to wait
        }

        bool Processor::failover(const uint64_t& now, const Utils::known_event_t& event) {
            auto& queue = *(event.queue);
            auto& queue_r2 = *(event.queue2);
            uint64_t item_len;
            auto _item = queue_r2.read(&item_len);

            if(_item == NULL) {
                // fprintf(stderr, "processor: empty queue\n");
                return false;
            }

            auto& kv = *(queue_r2.kv);
            auto item = parse_queue_item(event, item_len, _item);
            auto cleanup = [&item, &_item](){
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
                        (char*)&(item->id_net),
                        sizeof(item->id_net),
                        "1", 1,
                        "1", 1
                    )) {
                        if(kv.remove((char*)&(item->id_net), sizeof(item->id_net))) {
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
                                "Can't remove key %llu of event %s: %s\n",
                                item->id,
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

            if(check_wip(now, *item) || !do_failover()) {
                queue_r2.sync_read_offset(false);
                cleanup();
                return false;
            }

            if(!key_removed) {
                key_removed = kv.remove((char*)&(item->id_net), sizeof(item->id_net));

                if(!key_removed) {
                    key_removed = !kv.check((char*)&(item->id_net), sizeof(item->id_net));
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

            if(_item == NULL) {
                // fprintf(stderr, "processor: empty queue\n");
                return false;
            }

            // TODO: batch event processing

            auto item = parse_queue_item(event, item_len, _item);
            auto cleanup = [&item, &_item](){
                delete item;
                free(_item);
            };

            if(check_wip(now, *item)) {
                // TODO: what should really happen here?
                // fprintf(stderr, "skip repl: no_failover flag is set\n");
                cleanup();
                queue.sync_read_offset(false);
                return false;
            }

            server.wip[item->id] = now;

            auto& queue_r2 = *(event.queue2);

            if(queue_r2.kv->add((char*)&(item->id_net), sizeof(item->id_net), "1", 1)) {
                queue_r2.write(item_len, _item);
                // fprintf(
                //     stderr,
                //     "Key %llu for event %s has been added to queue2\n",
                //     item->id,
                //     event.id
                // );

            } else {
                // fprintf(
                //     stderr,
                //     "Key %llu for event %s already exists in queue2\n",
                //     item->id,
                //     event.id
                // );
            }

            queue.sync_read_offset();
            // fprintf(stderr, "processor: after sync_read_offset(), rid: %llu\n", item->id);

            return true;
        }
    }
}
