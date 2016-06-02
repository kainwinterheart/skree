#pragma once

#include <pthread.h>
#include <unordered_map>

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
            pthread_mutex_t mutex; // yep
        public:
            AtomicHashMap() : std::unordered_map<Key, Value, Hasher, Comparator>() {
                pthread_mutex_init(&mutex, nullptr);
            }

            virtual ~AtomicHashMap() {
                pthread_mutex_destroy(&mutex);
            }

            virtual typename std::unordered_map<Key, Value, Hasher, Comparator>::iterator lock() {
                pthread_mutex_lock(&mutex);
                return std::unordered_map<Key, Value, Hasher, Comparator>::end();
            }

            virtual void unlock() {
                pthread_mutex_lock(&mutex);
            }
        };
    }
}
