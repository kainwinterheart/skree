#ifndef _SKREE_WORKERS_PROCESSOR_H_
#define _SKREE_WORKERS_PROCESSOR_H_

#include "../base/worker.hpp"
#include "../utils/misc.hpp"
#include "../db_wrapper.hpp"

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
            virtual uint32_t process(const uint64_t& now, Utils::known_event_t& event) const;
            virtual void Stat(Utils::known_event_t& event, uint32_t count) const;

        protected:
            bool do_failover(
                const uint64_t& now,
                Utils::known_event_t& event,
                uint64_t itemId,
                std::shared_ptr<Utils::muh_str_t> item,
                DbWrapper::TSession& kv_session,
                DbWrapper::TSession& queue_session
            ) const;
        };
    }
}

// #include "../server.hpp" // sorry

#endif
