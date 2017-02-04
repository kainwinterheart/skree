#pragma once

#include <stdlib.h>
#include <vector>
#include <atomic>
#include "spin_lock.hpp"

namespace Skree {
    namespace Utils {
        template<typename Value>
        class RoundRobinVector : public std::vector<Value> {
        private:
            Utils::TSpinLock mutex;
            uint64_t pos;

            Value next_impl() {
                if(std::vector<Value>::empty())
                    throw std::logic_error ("next() called on empty round-robin vector");

                Utils::TSpinLockGuard guard (mutex);

                if(pos >= std::vector<Value>::size())
                    pos = 0;

                const auto& value = std::vector<Value>::at(pos);
                ++pos;

                return value;
            }
        public:
            RoundRobinVector() : std::vector<Value>() {
                pos = 0;
            }

            virtual ~RoundRobinVector() {
            }

            inline Value next();
        };
    }
}
