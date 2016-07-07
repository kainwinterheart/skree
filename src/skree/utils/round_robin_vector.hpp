#pragma once

#include <pthread.h>
#include <vector>

namespace Skree {
    namespace Utils {
        template<typename Value>
        class RoundRobinVector : public std::vector<Value> {
        private:
            pthread_mutex_t mutex;
            uint64_t pos;
        public:
            RoundRobinVector() : std::vector<Value>() {
                pthread_mutex_init(&mutex, nullptr);
                pos = 0;
            }

            virtual ~RoundRobinVector() {
                pthread_mutex_destroy(&mutex);
            }

            Value next() {
                if(std::vector<Value>::empty()) {
                    throw new std::logic_error ("next() called on empty round-robin vector");
                }

                pthread_mutex_lock(&mutex);

                if(pos >= std::vector<Value>::size())
                    pos = 0;

                const auto& value = std::vector<Value>::at(pos);
                ++pos;

                pthread_mutex_unlock(&mutex);

                return value;
            }
        };
    }
}
