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
            bool failover(const uint64_t& now, Utils::known_event_t& event);
            uint32_t process(const uint64_t& now, Utils::known_event_t& event);

            bool do_failover(
                const uint64_t& now,
                Utils::known_event_t& event,
                uint64_t itemId,
                std::shared_ptr<Utils::muh_str_t> item
            );
        };
    }
}

#include "../server.hpp" // sorry

#endif
