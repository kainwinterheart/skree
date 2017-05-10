#pragma once

#include "string.hpp"
#include "spin_lock.hpp"

// #include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unordered_map>
#include <vector>
#include <unordered_set>
#include <queue>
#include <utility>
#include <errno.h>
#include <functional>

namespace Skree {
    namespace Utils {
        class TSpinLock;

        class TForkManager {
        public:
            class TShmObject {
            public:
                enum EType {
                    SHMOT_DATA = 1
                };

            private:
                EType Type;

                Utils::muh_str_t Name;
                int Fd;
                uint32_t Len;
                void* Addr;
                int Pid;

            public:
                TShmObject(
                    const uint32_t len,
                    const TShmObject::EType type,
                    uint64_t pid = 0
                );

                TShmObject(const TShmObject& right) = delete;
                TShmObject& operator=(const TShmObject& right) = delete;

                TShmObject(TShmObject&& right)
                    : Type(right.Type)
                    , Name(std::move(right.Name))
                    , Fd(right.Fd)
                    , Len(right.Len)
                    , Addr(right.Addr)
                    , Pid(right.Pid)
                {
                    right.Fd = -1;
                    right.Addr = nullptr;
                }

                TShmObject& operator=(TShmObject&& right) {
                    Type = right.Type;
                    Name = std::move(right.Name);
                    Fd = right.Fd;
                    Len = right.Len;
                    Addr = right.Addr;
                    Pid = right.Pid;

                    right.Fd = -1;
                    right.Addr = nullptr;

                    return *this;
                }

                ~TShmObject();

                void* operator*() const {
                    return Addr;
                }

                int GetPid() const {
                    return Pid;
                }

                uint32_t GetLen() const {
                    return Len;
                }

                TShmObject::EType GetType() const {
                    return Type;
                }
            };

        private:
            unsigned int MaxWorkerCount;
            unsigned int CurrentWorkerCount;
            std::function<bool(void*)> Cb;
            std::unordered_map<int, int> Fd2Pid;
            std::vector<int> Fds;
            std::unordered_set<int> FreeWorkers;
            Utils::TSpinLock FreeWorkersLock;
            int WakeupFds[2];
            std::unordered_map<int, TShmObject> ShmObjects;
            Utils::TSpinLock ShmObjectsLock;
            std::queue<int> Waiters;
            Utils::TSpinLock WaitersLock;

        public:
            TForkManager(unsigned int maxWorkerCount, decltype(Cb)&& cb);

            int WaitFreeWorker();
            void FinalizeShmObject(TShmObject&& object);
            void Start();

            template<typename... TArgs>
            TShmObject GetShmObject(const int fd, TArgs&&... rest) const {
                return TShmObject(std::forward<TArgs&&>(rest)..., Fd2Pid.at(fd));
            }

        private:
            void RespawnWorkers();
            std::shared_ptr<TShmObject> GetShmObjectFor(const int pid, const bool erase);
            bool HaveShmObjectFor(const int pid);
            void PingWaiter();
        };
    }
}
