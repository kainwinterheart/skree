#pragma once

#include "string.hpp"
#include "spin_lock.hpp"
#include "events.hpp"
#include "hashers.hpp"

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
                std::function<void()> FinishCb;

            public:
                TShmObject() = delete;

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
                    , FinishCb(std::move(right.FinishCb))
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
                    FinishCb = std::move(right.FinishCb);

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

                void SetFinishCb(decltype(FinishCb)&& finishCb) {
                    FinishCb = std::move(finishCb);
                }

                void ExecFinishCb();
            };

            class TFreeWorker {
            private:
                int Fd;
                int Pid;
                bool Used;
                TForkManager& ForkManager;

            public:
                TFreeWorker(int fd, int pid, TForkManager& forkManager)
                    : Fd(fd)
                    , Pid(pid)
                    , Used(false)
                    , ForkManager(forkManager)
                {
                }

                TFreeWorker(const TFreeWorker&) = delete;

                TFreeWorker(TFreeWorker&& right)
                    : Fd(right.Fd)
                    , Pid(right.Pid)
                    , Used(right.Used)
                    , ForkManager(right.ForkManager)
                {
                    right.Used = true;
                }

                template<typename... TArgs>
                TShmObject GetShmObject(TArgs&&... rest) const {
                    return TShmObject(std::forward<TArgs&&>(rest)..., Pid);
                }

                void Use() {
                    Used = true;
                }

                ~TFreeWorker() {
                    if(!Used) {
                        {
                            Utils::TSpinLockGuard guard(ForkManager.FreeWorkersLock);

                            ForkManager.FreeWorkers.insert(std::make_pair(Fd, Pid));
                        }

                        ForkManager.PingWaiter();
                    }
                }
            };

            friend class TFreeWorker;

        private:
            unsigned int MaxWorkerCount;
            unsigned int CurrentWorkerCount;
            skree_module_t* Module;
            std::unordered_set<std::pair<int, int>, TPairHasher<int>> Workers;
            std::unordered_set<std::pair<int, int>, TPairHasher<int>> FreeWorkers;
            Utils::TSpinLock FreeWorkersLock;
            int WakeupFds[2];
            std::unordered_map<int, TShmObject> ShmObjects;
            std::unordered_map<int, std::shared_ptr<TShmObject>> UsedShmObjects;
            Utils::TSpinLock ShmObjectsLock;
            std::queue<int> Waiters;
            Utils::TSpinLock WaitersLock;

        public:
            TForkManager(unsigned int maxWorkerCount, skree_module_t* module);

            TForkManager::TFreeWorker WaitFreeWorker();
            void FinalizeShmObject(TShmObject&& object);
            void Start();

        private:
            void RespawnWorkers();
            std::shared_ptr<TShmObject> GetShmObjectFor(const int pid);
            std::shared_ptr<TShmObject> EraseShmObjectFor(const int pid);
            bool HaveShmObjectFor(const int pid);
            void PingWaiter();
        };
    }
}
