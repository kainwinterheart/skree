#pragma once

#include "hashers.hpp"
#include "string.hpp"
#include "atomic_hash_map.hpp"

#include <memory>

namespace Skree {
    class QueueDb;

    namespace Utils {
        typedef AtomicHashMap<
            std::shared_ptr<muh_str_t>,
            uint64_t,
            Utils::TMuhStrPointerHasher,
            Utils::TMuhStrPointerComparator
        > failover_t;

        typedef AtomicHashMap<
            std::shared_ptr<muh_str_t>,
            uint64_t,
            Utils::TMuhStrPointerHasher,
            Utils::TMuhStrPointerComparator
        > no_failover_t;

        struct skree_module_t {
            size_t path_len;
            char* path;
            const void* config;
        };

        struct event_group_t {
            size_t name_len;
            char* name;
            skree_module_t* module;
        };

        struct known_event_t {
            uint32_t id_len;
            uint32_t id_len_net;

            char* id;

            event_group_t* group;

            uint32_t ttl;
            uint32_t BatchSize;

            QueueDb* queue;
            QueueDb* queue2;
            QueueDb* r_queue;
            QueueDb* r2_queue;

            std::atomic<uint_fast64_t> stat_num_processed;
            std::atomic<uint_fast64_t> stat_num_failovered;

            failover_t failover;
            no_failover_t no_failover;

            void unfailover(const std::shared_ptr<muh_str_t>& failover_key) {
                {
                    auto failover_end = failover.lock();
                    auto it = failover.find(failover_key);

                    if(it != failover_end)
                        failover.erase(it);

                    failover.unlock();
                }

                {
                    auto no_failover_end = no_failover.lock();
                    auto it = no_failover.find(failover_key);

                    if(it != no_failover_end)
                        no_failover.erase(it);

                    no_failover.unlock();
                }
            }
        };

        typedef std::unordered_map<
            const char*,
            known_event_t*,
            TCharPointerHasher,
            TCharPointerComparator
        > known_events_t;

        typedef std::unordered_map<
            const char*,
            skree_module_t*,
            TCharPointerHasher,
            TCharPointerComparator
        > skree_modules_t;

        typedef std::unordered_map<
            const char*,
            event_group_t*,
            TCharPointerHasher,
            TCharPointerComparator
        > event_groups_t;
    }
}
