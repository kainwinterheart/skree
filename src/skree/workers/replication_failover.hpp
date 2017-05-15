#pragma once

#include "replication.hpp"

namespace Skree {
    namespace Workers {
        class ReplicationFailover : public Skree::Workers::Replication {
        public:
            ReplicationFailover(Skree::Server& _server, void* _args = nullptr)
                : Skree::Workers::Replication(_server, _args) {}
        private:

            virtual uint32_t process(const uint64_t& now, Utils::known_event_t& event) const override;
            virtual void Stat(Utils::known_event_t& event, uint32_t count) const override;
        };
    }
}
