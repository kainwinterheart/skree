#pragma once

#include <stdlib.h>
#include <unordered_map>
#include <atomic>

namespace Skree {
    namespace Utils {
        template<
            typename Key,
            typename Value,
            typename Hasher = std::hash<Key>,
            typename Comparator = std::equal_to<Key>
        >
        class AtomicHashMap : public std::unordered_map<Key, Value, Hasher, Comparator> {
        private:
            std::atomic<bool> mutex; // yep
        public:
            AtomicHashMap() : std::unordered_map<Key, Value, Hasher, Comparator>() {
                mutex = false;
            }

            AtomicHashMap(const AtomicHashMap& right)
                : std::unordered_map<Key, Value, Hasher, Comparator>(right) {
                mutex = false;
            }

            virtual ~AtomicHashMap() {
            }

            virtual typename std::unordered_map<Key, Value, Hasher, Comparator>::iterator lock() {
                while(mutex.exchange(true)) {
                    continue;
                }

                return std::unordered_map<Key, Value, Hasher, Comparator>::end();
            }

            virtual void unlock() {
                if(!mutex.exchange(false)) {
                    abort();
                }
            }
        };
    }
}
