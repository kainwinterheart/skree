#pragma once

#include <stdlib.h>
#include <vector>
#include <atomic>

namespace Skree {
    namespace Utils {
        template<typename Value>
        class RoundRobinVector : public std::vector<Value> {
        private:
            std::atomic<bool> mutex;
            uint64_t pos;

            Value next_impl() {
                if(std::vector<Value>::empty())
                    throw std::logic_error ("next() called on empty round-robin vector");

                while(mutex.exchange(true)) {
                    continue;
                }

                if(pos >= std::vector<Value>::size())
                    pos = 0;

                const auto& value = std::vector<Value>::at(pos);
                ++pos;

                if(!mutex.exchange(false)) {
                    abort();
                }

                return value;
            }
        public:
            RoundRobinVector() : std::vector<Value>() {
                mutex = false;
                pos = 0;
            }

            virtual ~RoundRobinVector() {
            }

            inline Value next();
        };
    }
}
