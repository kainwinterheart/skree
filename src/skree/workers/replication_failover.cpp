#include "replication_failover.hpp"
#include "../queue_db.hpp"
#include "../server.hpp"
#include "../meta/states.hpp"

namespace Skree {
    namespace Workers {
        void ReplicationFailover::Stat(Utils::known_event_t& event, uint32_t count) const {
            server.stat_num_repl_it += count;
        }

        uint32_t ReplicationFailover::process(const uint64_t& now, Utils::known_event_t& event) const {
            auto queue_session = event.r_queue->kv->NewSession(DbWrapper::TSession::ST_QUEUE);
            auto queue2_session = event.r2_queue->kv->NewSession(DbWrapper::TSession::ST_QUEUE);
            auto kv_session = event.r_queue->kv->NewSession(DbWrapper::TSession::ST_KV);

            std::deque<std::tuple<
                uint64_t,
                std::shared_ptr<Utils::muh_str_t>,
                std::shared_ptr<QueueItem>
            >> itemsToFailover;
            std::deque<uint64_t> idsToRemove;

            bool waited = false;
            auto& failover = event.failover;
            uint32_t processed = 0;

            for(uint32_t i = 0; i < event.BatchSize; ++i) {
                uint64_t itemId;
                auto _item = queue2_session->next(itemId);

                if(_item) {
                    auto item = parse_queue_item(event,/* itemId,*/ _item);

                    auto failover_end = failover.lock();
                    auto it = failover.find(item->failover_key);
                    failover.unlock();

                    if(
                        (it == failover_end)
                        || !check_no_failover(now, *item, event)
                    ) {
                        itemsToFailover.push_back(std::make_tuple(itemId, _item, item));

                    } else {
                        idsToRemove.push_back(itemId);
                    }

                    ++processed;

                } else if((i > 0) && !waited) { // TODO: is this really necessary here?
                    waited = true;
                    // nanosleep(); // TODO
                    --i;
                    continue;

                } else {
                    break;
                }
            }

            if(!itemsToFailover.empty() || !idsToRemove.empty()) {
                queue_session->Reset();
                queue2_session->Reset();
                kv_session->Reset();
            }

            if(!itemsToFailover.empty()) {
                for(const auto& node : itemsToFailover) {
                    const auto& [itemId, _item, item] = node;

                    if(do_failover(
                        _item->len,
                        _item->data,
                        *item,
                        event,
                        *kv_session,
                        *queue_session
                    )) {
                        idsToRemove.push_back(itemId);
                    }
                }

                if(!idsToRemove.empty()) {
                    queue_session->Reset();
                    kv_session->Reset();
                }
            }

            if(!idsToRemove.empty()) {
                for(const auto id : idsToRemove) {
                    queue2_session->remove(id);
                }

                // queue2_session->Reset();
            }

            return processed;
        }
    }
}
