#pragma once

#include <atomic>

namespace Skree {
    namespace Utils {
        class TSpinLock {
        private:
            std::atomic_flag Lock_ = ATOMIC_FLAG_INIT;

        public:
            TSpinLock() = default;
            TSpinLock(const TSpinLock&) = delete;
            TSpinLock(TSpinLock&&) = delete;

            void Lock() {
                while(Lock_.test_and_set(std::memory_order_acquire)) {
                    continue;
                }
            }

            void Unlock() {
                Lock_.clear(std::memory_order_release);
            }
        };

        class TSpinLockGuard {
        private:
            TSpinLock& Lock;

        public:
            TSpinLockGuard(TSpinLock& lock) : Lock(lock) {
                Lock.Lock();
            }

            TSpinLockGuard() = delete;
            TSpinLockGuard(const TSpinLockGuard&) = delete;
            TSpinLockGuard(TSpinLockGuard&&) = delete;

            ~TSpinLockGuard() {
                Lock.Unlock();
            }
        };
    }
}
