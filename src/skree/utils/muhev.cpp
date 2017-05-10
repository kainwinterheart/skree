#include "muhev.hpp"

// #include <stdlib.h>
#include <unistd.h>
#include <errno.h>

#ifdef __linux__
#include <sys/epoll.h>
#include <sys/ioctl.h>
#include <utility>
#else
#include <sys/types.h>
#include <sys/event.h>
#endif

namespace Skree {
    namespace NMuhEv {
        TEvSpec GetEvent(const TEvList& list, int index) {
            const auto& event = (list.Triggered.get())[index];

            TEvSpec out {
        #ifdef __linux__
                .Ident = (uintptr_t)event.data.fd,
                .Ctx = list.FdMap.at(event.data.fd),
        #else
                .Ident = event.ident,
                .Ctx = event.udata,
        #endif
                .Filter = MUHEV_FILTER_NONE,
                .Flags = MUHEV_FLAG_NONE,
            };
        #ifdef __linux__
            // Utils::cluck(1, "woot");
            if(event.events & EPOLLIN)
                out.Filter |= MUHEV_FILTER_READ;

            if(event.events & EPOLLOUT)
                out.Filter |= MUHEV_FILTER_WRITE;
        #else
            switch(event.filter) {
                case EVFILT_READ:
                    out.Filter = MUHEV_FILTER_READ;
                    break;
                case EVFILT_WRITE:
                    out.Filter = MUHEV_FILTER_WRITE;
                    break;
                default:
                    out.Flags |= MUHEV_FLAG_ERROR;
            }

            if(event.flags & EV_ERROR) {
                out.Flags |= MUHEV_FLAG_ERROR;
            }

            if(event.flags & EV_EOF) {
                out.Flags |= MUHEV_FLAG_EOF;
            }
        #endif
            return out;
        }

        namespace {
            static inline std::shared_ptr<TInternalEvStruct> MakeIntEvList(int count) {
                return std::shared_ptr<TInternalEvStruct>(
                    new TInternalEvStruct[count],
                    std::default_delete<TInternalEvStruct[]>()
                );
            }

        #ifndef __linux__
            static inline void AddEventKqueueImpl(int filter, const TEvSpec& spec, TInternalEvStruct* event) {
                EV_SET(
                    event,
                    spec.Ident,
                    filter,
                    EV_ADD | EV_ENABLE | EV_ONESHOT,
                    0,
                    0,
                    0
                );

                event->udata = spec.Ctx;
            }
        #endif
        }

        TEvList MakeEvList(int count) {
        #ifdef __linux__
            std::unordered_map<uintptr_t, void*> fdMap;
            fdMap.reserve(count);
        #else
            count *= 2;
        #endif
            return TEvList {
                .Count = count,
                .Pos = 0,
        #ifdef __linux__
                .Events = std::shared_ptr<TInternalEvStruct>(),
                .Triggered = MakeIntEvList(count),
                .FdMap = std::move(fdMap),
        #else
                .Events = MakeIntEvList(count),
                .Triggered = MakeIntEvList(count),
        #endif
            };
        }

        TLoop::TLoop() {
        #ifdef __linux__
            QueueId = epoll_create(0x1);
        #else
            QueueId = kqueue();
        #endif
            if(QueueId == -1) {
                perror("kqueue");
                abort();
            }
        }

        TLoop::~TLoop() {
            if(close(QueueId) == -1) {
                perror("close");
            }
        }

        void TLoop::AddEvent(const TEvSpec& spec, TEvList& list) {
        #ifdef __linux__
            TInternalEvStruct event = { 0 };
            event.events = EPOLLONESHOT;
            event.data.fd = spec.Ident;

            if(spec.Filter & MUHEV_FILTER_READ)
                event.events |= EPOLLIN;

            if(spec.Filter & MUHEV_FILTER_WRITE)
                event.events |= EPOLLOUT;

            if(
                (epoll_ctl(QueueId, EPOLL_CTL_MOD, spec.Ident, &event) != 0)
                && (
                    (
                        (errno == ENOENT)
                        && (epoll_ctl(QueueId, EPOLL_CTL_ADD, spec.Ident, &event) != 0)
                    )
                    || (errno != ENOENT)
                )
            ) {
                perror("epoll_ctl");
                abort();
            }

            ++list.Pos;
            list.FdMap[spec.Ident] = spec.Ctx;
        #else
            if(spec.Filter & MUHEV_FILTER_READ) {
                auto& event = (list.Events.get())[list.Pos++];
                AddEventKqueueImpl(EVFILT_READ, spec, &event);
            }

            if(spec.Filter & MUHEV_FILTER_WRITE) {
                auto& event = (list.Events.get())[list.Pos++];
                AddEventKqueueImpl(EVFILT_WRITE, spec, &event);
            }
        #endif
        }

        int TLoop::Wait(TEvList& list) {
            int triggeredCount = 0;

            if(list.Pos < 1) {
                return triggeredCount;
            }

            while(true) {
        #ifdef __linux__
                // Utils::cluck(1, "wait()");
                triggeredCount = epoll_wait(
                    QueueId,
                    list.Triggered.get(),
                    list.Pos,
                    24 * 60 * 60
                );
        #else
                triggeredCount = kevent(
                    QueueId,
                    list.Events.get(),
                    list.Pos,
                    list.Triggered.get(),
                    list.Pos,
                    nullptr
                );
        #endif
                if(triggeredCount < 0) {
                    if(errno != EINTR) {
                        perror("kevent");
                        abort();
                    }

                } else {
                    return triggeredCount;
                }
            }
        }
    }
}
