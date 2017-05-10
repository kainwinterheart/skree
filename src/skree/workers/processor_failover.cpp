#include "processor_failover.hpp"
#include "../queue_db.hpp"
#include "../server.hpp"
#include "../meta/states.hpp"
#include <ctime>
#include <limits>

namespace Skree {
    namespace Workers {
        uint32_t ProcessorFailover::process(const uint64_t& now, Utils::known_event_t& event) const {
            // auto& queue_r2 = *(event.queue2);
            auto kv_session = event.queue->kv->NewSession(DbWrapper::TSession::ST_KV);
            auto queue_session = event.queue->kv->NewSession(DbWrapper::TSession::ST_QUEUE);
            auto queue2_session = event.queue2->kv->NewSession(DbWrapper::TSession::ST_QUEUE);
            uint32_t processed = 0;

            struct TRemoveListItem {
                uint64_t ItemId;
                uint64_t OrigItemId;
                bool KeyRemoved;
            };

            std::deque<TRemoveListItem> removeList;

            for(uint32_t i = 0; i < event.BatchSize; ++i) {
                uint64_t itemId;
                auto item = queue2_session->next(itemId);

                if(!item) {
                    // Utils::cluck(1, "processor: empty queue\n");
                    break;
                }

                uint64_t origItemId;
                memcpy(&origItemId, item->data, sizeof(origItemId));
                origItemId = ntohll(origItemId);

                uint32_t itemLen = item->len - sizeof(origItemId);
                char* itemData = item->data + sizeof(origItemId);

                auto state = server.get_event_state(origItemId, event, now, kv_session.get());
                bool repeat = false;
                bool key_removed = false;
                // short reason = 0;

                if(
                    (state == SKREE_META_EVENTSTATE_PENDING)
                    || (state == SKREE_META_EVENTSTATE_PROCESSING)
                ) {
                    repeat = true;

                } else if(state == SKREE_META_EVENTSTATE_LOST) {
                    std::shared_ptr<Utils::muh_str_t> _item(new Utils::muh_str_t(itemData, itemLen, false));
                    _item->origin = item;

                    if(do_failover(now, event, origItemId, _item, *kv_session, *queue_session)) {
                        key_removed = true;
                        // reason = 1;

                    } else {
                        repeat = true;
                    }
                }

                if(repeat) {
                    Utils::cluck(3, "[ProcessorFailover::failover] repeat: %llu, state: %u\n", origItemId, state);
                    break;
                }

                removeList.push_back(TRemoveListItem {
                    .ItemId = itemId,
                    .OrigItemId = origItemId,
                    .KeyRemoved = key_removed
                });

                ++processed;
            }

            // queue_r2.Reset();
            kv_session->Reset();
            queue_session->Reset();
            queue2_session->Reset();

            for (const auto& item : removeList) {
                bool key_removed = item.KeyRemoved;
                auto origItemIdNet = htonll(item.OrigItemId);

                if(!key_removed) {
                    key_removed = kv_session->remove((char*)&origItemIdNet, sizeof(origItemIdNet));
                    // reason = 2;

                    if(!key_removed) {
                        key_removed = (kv_session->check((char*)&origItemIdNet, sizeof(origItemIdNet)) <= 0);
                        // reason = 3;
                    }
                }

                if(key_removed) {
                    key_removed = queue_session->remove(item.OrigItemId);
                    // reason = 4;

                    if(!key_removed) {
                        key_removed = (queue_session->check(item.OrigItemId) <= 0);
                        // reason = 5;
                    }
                }

                if(key_removed) {
                    key_removed = queue2_session->remove(item.ItemId);
                    // reason = 6;

                    if(!key_removed) {
                        key_removed = (queue2_session->check(item.ItemId) <= 0);
                        // reason = 7;
                    }
                }

                // if(key_removed) {
                //     Utils::cluck(3, "[ProcessorFailover::failover] key %llu removed, reason: %d\n", item->id, reason);
                // }



                // if(key_removed) {
                //     event.queue2->kv->remove((char*)&itemIdNet, sizeof(itemIdNet));
                // }
                // cleanup();
            }

            return processed;
        }

        void ProcessorFailover::Stat(Utils::known_event_t& event, uint32_t count) const {
            event.stat_num_failovered += count;
        }
    }
}
