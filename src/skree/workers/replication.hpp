#pragma once

#include "../base/worker.hpp"
#include "../utils/misc.hpp"
#include "../pending_reads/replication/propose_self.hpp"
#include "../pending_reads/replication/ping_task.hpp"

#include <vector>
#include <string>

namespace Skree {
    namespace Workers {
        class Replication : public Skree::Base::Worker {
        public:
            Replication(Skree::Server& _server, const void* _args = nullptr)
                : Skree::Base::Worker(_server, _args) {}

            virtual void run() override;
        private:
            struct QueueItem {
                uint32_t rin_len;
                uint32_t hostname_len;
                const char* rin;
                uint64_t rts;
                uint64_t rid_net;
                uint64_t rid;
                char* hostname;
                uint32_t port;
                uint32_t peers_cnt;
                char* rpr;
                uint32_t peer_id_len;
                uint32_t failover_key_len;
                char* peer_id;
                char* failover_key;
            };

            static std::shared_ptr<Replication::QueueItem> parse_queue_item(
                Utils::known_event_t& event,
                const char* item
            );

            bool failover(const uint64_t& now, Utils::known_event_t& event);
            bool replication(const uint64_t& now, Utils::known_event_t& event);
            bool check_no_failover(
                const uint64_t& now,
                const Replication::QueueItem& item,
                Utils::known_event_t& event
            );

            bool do_failover(
                const uint64_t& raw_item_len,
                char*& raw_item,
                const Replication::QueueItem& item,
                Utils::known_event_t& event
            );
        };
    }
}

#include "../server.hpp" // sorry
