#pragma once

#include <stdlib.h>
#include <unordered_map>
#include <memory>
#include "spin_lock.hpp"

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
            std::shared_ptr<Utils::TSpinLock> mutex; // yep

        public:
            AtomicHashMap() : std::unordered_map<Key, Value, Hasher, Comparator>() {
                mutex.reset(new Utils::TSpinLock());
            }

            AtomicHashMap(const AtomicHashMap& right)
                : std::unordered_map<Key, Value, Hasher, Comparator>(right) {
                mutex.reset(new Utils::TSpinLock());
            }

            virtual ~AtomicHashMap() {
            }

            virtual typename std::unordered_map<Key, Value, Hasher, Comparator>::iterator lock() {
                mutex->Lock();

                return std::unordered_map<Key, Value, Hasher, Comparator>::end();
            }

            virtual void unlock() {
                mutex->Unlock();
            }
        };
    }
}
