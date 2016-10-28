#pragma once

#include <memory>
#include <stdlib.h>
#include <errno.h>

#ifdef __linux__
#include <sys/epoll.h>
#include <sys/ioctl.h>
#else
#include <sys/types.h>
#include <sys/event.h>
#endif

namespace Skree {
    namespace NMuhEv {
        enum EEvFilter {
            MUHEV_FILTER_NONE = 0,
            MUHEV_FILTER_READ = 2,
            MUHEV_FILTER_WRITE = 4
        };

        enum EEvFlags {
            MUHEV_FLAG_NONE = 0,
            MUHEV_FLAG_ERROR = 2,
            MUHEV_FLAG_EOF = 4
        };

        struct TEvSpec {
            uintptr_t Ident;
            int Filter;
            int Flags;
            void* Ctx;
            intptr_t Data;
        };
#ifdef __linux__
        using TInternalEvStruct = struct epoll_event;
#else
        using TInternalEvStruct = struct kevent;
#endif
        struct TEvList {
            int Count;
            int Pos;
            std::shared_ptr<TInternalEvStruct> Events;
            std::shared_ptr<TInternalEvStruct> Triggered;
        };

        static inline TEvSpec GetEvent(const TEvList& list, int index) {
            const auto& event = (list.Triggered.get())[index];

            TEvSpec out {
#ifdef __linux__
                .Ident = (uintptr_t)event.data.fd,
                .Ctx = event.data.ptr,
                .Data = 0,
#else
                .Ident = event.ident,
                .Ctx = event.udata,
                .Data = event.data,
#endif
                .Filter = MUHEV_FILTER_NONE,
                .Flags = MUHEV_FLAG_NONE,
            };
#ifdef __linux__
            if(event.events & EPOLLIN) {
                int bytesAvailable = 0;

                if(ioctl(out.Ident, FIONREAD, &bytesAvailable) == -1)
                    perror("ioctl");

                out.Data = bytesAvailable;
                out.Filter |= MUHEV_FILTER_READ;
            }

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
                    throw std::logic_error ("bad filter");
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
        }

        static inline TEvList MakeEvList(int count) {
#ifndef __linux__
            count *= 2;
#endif
            return TEvList {
                .Count = count,
                .Pos = 0,
#ifdef __linux__
                .Events = std::shared_ptr<TInternalEvStruct>(),
                .Triggered = MakeIntEvList(count),
#else
                .Events = MakeIntEvList(count),
                .Triggered = MakeIntEvList(count),
#endif
            };
        }

        class TLoop {
        private:
            int QueueId;

        public:
            TLoop() {
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

            ~TLoop() {
            }

        private:
#ifndef __linux__
            static inline void AddEventKqueueImpl(
                int filter,
                const TEvSpec& spec,
                TInternalEvStruct* event
            ) {
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
        public:
            void AddEvent(const TEvSpec& spec, TEvList& list) {
#ifdef __linux__
                TInternalEvStruct event = { 0 };
                event.events = EPOLLONESHOT;
                event.data.ptr = spec.Ctx;
                event.data.fd = spec.Ident;

                if(spec.Filter & MUHEV_FILTER_READ)
                    event.events |= EPOLLIN;

                if(spec.Filter & MUHEV_FILTER_WRITE)
                    event.events |= EPOLLOUT;

                if(
                    (epoll_ctl(QueueId, EPOLL_CTL_ADD, spec.Ident, &event) != 0)
                    && (
                        (
                            (errno == EEXIST)
                            && (epoll_ctl(QueueId, EPOLL_CTL_MOD, spec.Ident, &event) != 0)
                        )
                        || (errno != EEXIST)
                    )
                ) {
                    perror("epoll_ctl");
                    abort();
                }

                ++list.Pos;
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

            int Wait(TEvList& list) {
                int triggeredCount = 0;

                if(list.Pos < 1) {
                    return triggeredCount;
                }

                while(true) {
#ifdef __linux__
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
        };
    }
}
