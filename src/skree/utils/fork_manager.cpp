#include "fork_manager.hpp"

#include "spin_lock.hpp"
#include "misc.hpp"
#include "muhev.hpp"

#include <dlfcn.h>

using namespace Skree::Utils;

TForkManager::TShmObject::TShmObject(const uint32_t len, const TShmObject::EType type, uint64_t pid)
    : Type(type)
    , Fd(-1)
    , Len(len)
    , Addr(nullptr)
    , Pid(pid)
{
    if(Pid == 0) {
        Pid = getpid();
    }

    Name.own = true;
    Name.data = (char*)malloc(12 + 20 + 1 + 1 + 1);

    sprintf(Name.data, "/skree_shm_%d_%d", Pid, (int)Type);
    Name.len = strlen(Name.data);

    Fd = shm_open(Name.data, O_RDWR | O_CREAT);

    if(Fd == -1) {
        perror("shm_open");
        abort();
    }

    Addr = mmap(nullptr, Len, PROT_READ | PROT_WRITE, MAP_SHARED, Fd, 0);

    if(Addr == MAP_FAILED) {
        perror("mmap");
        abort();
    }
}

TForkManager::TShmObject::~TShmObject() {
    if(Addr && (Addr != MAP_FAILED) && (munmap(Addr, Len) == -1)) {
        perror("munmap");
    }

    if((Fd != -1) && (shm_unlink(Name.data) == -1)) {
        perror("shm_unlink");
    }
}

TForkManager::TForkManager(unsigned int maxWorkerCount, skree_module_t* module)
    : MaxWorkerCount(maxWorkerCount)
    , CurrentWorkerCount(0)
    , Module(module)
{
}

void TForkManager::RespawnWorkers() {
    for(; CurrentWorkerCount < MaxWorkerCount; ++CurrentWorkerCount) {
        int fds[2];

        if(socketpair(PF_LOCAL, SOCK_STREAM, 0, fds) == -1) {
            perror("socketpair");
            abort();
        }

        int pid = fork();

        if (pid > 0) {
            close(fds[0]);

            Fd2Pid[fds[1]] = pid;
            Fds.push_back(fds[1]);

        } else if (pid == 0) {
            close(fds[1]);

            auto* handle = dlopen(Module->path, RTLD_NOW | RTLD_LOCAL | RTLD_FIRST);

            if(!handle) {
                Utils::cluck(1, dlerror());
                abort();
            }

            auto* initializer = (void(*)(const void*))dlsym(handle, "init");

            if(!initializer) {
                Utils::cluck(1, dlerror());
                abort();
            }

            auto* deinitializer = (void(*)())dlsym(handle, "destroy");

            if(!deinitializer) {
                Utils::cluck(1, dlerror());
                abort();
            }

            if(atexit(deinitializer) == -1) {
                perror("atexit");
                abort();
            }

            auto* processor = (bool(*)(uint32_t, void*))dlsym(handle, "main");

            if(!processor) {
                Utils::cluck(1, dlerror());
                abort();
            }

            initializer(Module->config);

            while (true) {
                const int result = ::write(fds[0], "?", 1);

                if (
                    (result == -1)
                    && (
                        (errno == EINTR)
                        || (errno == EAGAIN)
                    )
                ) {
                    continue;
                }

                if (result != 1) {
                    if (result == -1) {
                        perror("write");
                    }

                    abort();
                }

                uint32_t messageSize;
                char* data = nullptr;
                size_t len = 0;

                while(len < sizeof(messageSize)) {
                    int read_len = ::read(fds[0], (&messageSize + len), (sizeof(messageSize) - len));

                    if(read_len > 0) {
                        len += read_len;

                    } else {
                        if(read_len == -1) {
                            perror("read");
                        }

                        abort();
                    }
                }

                TShmObject shmObject(ntohl(messageSize), TShmObject::SHMOT_DATA);

                len = 0;
                uint32_t eventCount;

                memcpy(&eventCount, *shmObject, sizeof(eventCount));
                len += sizeof(eventCount);

                if(processor(eventCount, (char*)*shmObject + sizeof(eventCount))) {
                    // TODO: handle success?

                } else {
                    // TODO: handle failure
                }

                while(true) {
                    const int result = ::write(fds[0], "!", 1);

                    if (
                        (result == -1)
                        && (
                            (errno == EINTR)
                            || (errno == EAGAIN)
                        )
                    ) {
                        continue;
                    }

                    if (result != 1) {
                        if (result == -1) {
                            perror("write");
                        }

                        abort();
                    }

                    break;
                }
            }

            exit(0);

        } else {
            perror("fork");
            abort();
        }
    }
}

std::shared_ptr<TForkManager::TShmObject> TForkManager::GetShmObjectFor(const int pid, const bool erase) {
    Utils::TSpinLockGuard guard(ShmObjectsLock);

    const auto& it = ShmObjects.find(pid);

    if(it == ShmObjects.end()) {
        return std::shared_ptr<TShmObject>();

    } else {
        std::shared_ptr<TShmObject> object(new TShmObject(std::move(it->second)));

        if(erase) {
            ShmObjects.erase(it);
        }

        return object;
    }
}

bool TForkManager::HaveShmObjectFor(const int pid) {
    Utils::TSpinLockGuard guard(ShmObjectsLock);

    const auto& it = ShmObjects.find(pid);

    return (it != ShmObjects.end());
}

TForkManager::TFreeWorker TForkManager::WaitFreeWorker() {
    NMuhEv::TLoop loop;
    int fds[2];

    if(socketpair(PF_LOCAL, SOCK_STREAM, 0, fds) == -1) {
        perror("socketpair");
        abort();
    }

    while(true) {
        if(!FreeWorkers.empty()) {
            Utils::TSpinLockGuard guard(FreeWorkersLock);

            if(!FreeWorkers.empty()) {
                const auto& it = FreeWorkers.begin();
                const int fd = *it;

                FreeWorkers.erase(it);

                return TFreeWorker(fd, *this);
            }
        }

        {
            Utils::TSpinLockGuard guard(WaitersLock);
            Waiters.push(fds[1]);
        }

        auto list = NMuhEv::MakeEvList(1);

        loop.AddEvent(NMuhEv::TEvSpec {
            .Ident = (uintptr_t)fds[0],
            .Filter = NMuhEv::MUHEV_FILTER_READ,
            .Flags = NMuhEv::MUHEV_FLAG_NONE,
            .Ctx = nullptr
        }, list);

        int triggeredCount = loop.Wait(list);

        if(triggeredCount < 0) {
            perror("kevent");
            abort();

        } else if(triggeredCount > 0) {
            for(int i = 0; i < triggeredCount; ++i) {
                const auto& event = NMuhEv::GetEvent(list, i);

                char dummy[128];
                int rv = recvfrom(
                    event.Ident,
                    dummy,
                    128,
                    MSG_DONTWAIT,
                    NULL,
                    0
                );

                if(rv < 0) {
                    perror("recvfrom");
                    abort();
                }
            }
        }
    }
}

void TForkManager::FinalizeShmObject(TShmObject&& object) {
    if(object.GetType() != TShmObject::SHMOT_DATA) {
        abort();
    }

    {
        Utils::TSpinLockGuard guard(ShmObjectsLock);

        ShmObjects.insert_or_assign(object.GetPid(), std::move(object));
    }

    while(true) {
        const int result = ::write(WakeupFds[1], "1", 1);

        if (
            (result == -1)
            && (
                (errno == EINTR)
                || (errno == EAGAIN)
            )
        ) {
            continue;
        }

        if (result != 1) {
            if (result == -1) {
                perror("write");
            }

            abort();
        }

        break;
    }
}

void TForkManager::Start() {
    NMuhEv::TLoop loop;

    if(socketpair(PF_LOCAL, SOCK_STREAM, 0, WakeupFds) == -1) {
        perror("socketpair");
        abort();
    }

    while(true) {
        RespawnWorkers();

        auto list = NMuhEv::MakeEvList(Fds.size() + 1);

        loop.AddEvent(NMuhEv::TEvSpec {
            .Ident = (uintptr_t)WakeupFds[0],
            .Filter = NMuhEv::MUHEV_FILTER_READ,
            .Flags = NMuhEv::MUHEV_FLAG_NONE,
            .Ctx = nullptr
        }, list);

        for(auto fd : Fds) {
            bool isFree = true;

            if(FreeWorkers.count(fd) > 0) {
                Utils::TSpinLockGuard guard(FreeWorkersLock);

                if(FreeWorkers.count(fd) > 0) {
                    isFree = true;
                }
            }

            if(isFree) {
                PingWaiter(); // Is this really necessary?
                continue;
            }

            loop.AddEvent(NMuhEv::TEvSpec {
                .Ident = (uintptr_t)fd,
                .Filter = (NMuhEv::MUHEV_FILTER_READ | (
                    HaveShmObjectFor(Fd2Pid[fd]) ? NMuhEv::MUHEV_FILTER_WRITE : 0
                )),
                .Flags = NMuhEv::MUHEV_FLAG_NONE,
                .Ctx = nullptr
            }, list);
        }

        int triggeredCount = loop.Wait(list);
        // Utils::cluck(3, "Got %d events in thread %llu", triggeredCount, Utils::ThreadId());
        if(triggeredCount < 0) {
            perror("kevent");
            abort();

        } else if(triggeredCount > 0) {
            for(int i = 0; i < triggeredCount; ++i) {
                const auto& event = NMuhEv::GetEvent(list, i);

                if(event.Flags & NMuhEv::MUHEV_FLAG_EOF) {
                    Utils::cluck(1, "EV_EOF");
                }

                if(event.Flags & NMuhEv::MUHEV_FLAG_ERROR) {
                    Utils::cluck(1, "EV_ERROR");
                }

                if(event.Ident == WakeupFds[0]) {
                    {
                        char dummy[128];
                        int rv = recvfrom(
                            event.Ident,
                            dummy,
                            128,
                            MSG_DONTWAIT,
                            NULL,
                            0
                        );

                        if(rv < 0) {
                            perror("recvfrom");
                            abort();
                        }
                    }

                } else {
                    if(
                        (event.Flags & NMuhEv::MUHEV_FLAG_EOF)
                        && (event.Flags & NMuhEv::MUHEV_FLAG_ERROR)
                    ) {
                        GetShmObjectFor(Fd2Pid[event.Ident], true);
                        continue;
                    }

                    if(event.Filter & NMuhEv::MUHEV_FILTER_WRITE) {
                        auto&& object = GetShmObjectFor(Fd2Pid[event.Ident], false);

                        if(object) {
                            size_t len = 0;
                            const uint32_t size = object->GetLen();

                            while(len < sizeof(size)) {
                                const int result = ::write(event.Ident, (&size) + len, sizeof(size));

                                if (
                                    (result == -1)
                                    && (
                                        (errno == EINTR)
                                        || (errno == EAGAIN)
                                    )
                                ) {
                                    continue;
                                }

                                if (result != 1) {
                                    if (result == -1) {
                                        perror("write");
                                    }

                                    abort();
                                }

                                len += size;
                            }
                        }
                    }

                    if(event.Filter & NMuhEv::MUHEV_FILTER_READ) {
                        char msg[128];
                        int rv = recvfrom(
                            event.Ident,
                            msg,
                            128,
                            MSG_DONTWAIT,
                            NULL,
                            0
                        );

                        if(rv < 0) {
                            perror("recvfrom");
                            abort();
                        }

                        for(int i = 0; i < rv; ++i) {
                            if(msg[i] == '?') {
                                {
                                    Utils::TSpinLockGuard guard(FreeWorkersLock);
                                    FreeWorkers.insert(event.Ident);
                                }

                                PingWaiter();

                            } else if(msg[i] == '!') {
                                GetShmObjectFor(Fd2Pid[event.Ident], true);
                                // TODO: finalize event processing here

                            } else {
                                abort();
                            }
                        }
                    }
                }
            }
        }
    }
}

void TForkManager::PingWaiter() {
    int fd = -1;

    if(!Waiters.empty()) {
        Utils::TSpinLockGuard guard(WaitersLock);

        if(!Waiters.empty()) {
            fd = Waiters.front();
            Waiters.pop();
        }
    }

    while(fd > -1) {
        const int result = ::write(fd, "1", 1);

        if (
            (result == -1)
            && (
                (errno == EINTR)
                || (errno == EAGAIN)
            )
        ) {
            continue;
        }

        if (result != 1) {
            if (result == -1) {
                perror("write");
            }

            abort();
        }

        break;
    }
}
