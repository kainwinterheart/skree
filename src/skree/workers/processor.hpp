#ifndef _SKREE_WORKERS_PROCESSOR_H_
#define _SKREE_WORKERS_PROCESSOR_H_

#include "../base/worker.hpp"
#include "../utils/misc.hpp"

#include <vector>
#include <string>

namespace Skree {
    namespace Workers {
        class Processor : public Skree::Base::Worker {
        public:
            Processor(Skree::Server& _server, const void* _args = nullptr)
                : Skree::Base::Worker(_server, _args) {}

            virtual void run() override;
        private:
            struct QueueItem {
                uint64_t id;
                uint64_t id_net;
                const char* data;
                uint64_t len; // TODO: 64 or 32?
            };

            static Processor::QueueItem* parse_queue_item(
                Utils::known_event_t& event,
                const uint64_t item_len,
                const char* item
            );

            bool failover(const uint64_t& now, Utils::known_event_t& event);
            bool process(const uint64_t& now, Utils::known_event_t& event);
            bool check_wip(const uint64_t& now, const Processor::QueueItem& item);

            bool do_failover(
                const uint64_t& now,
                Utils::known_event_t& event,
                const Processor::QueueItem& item
            );
        };
    }
}

#include "../server.hpp" // sorry

#endif
