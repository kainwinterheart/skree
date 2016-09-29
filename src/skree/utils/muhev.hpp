#pragma once

#include <memory>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/event.h>
// #include <sys/time.h>

namespace Skree {
    namespace NMuhEv {
        enum EEvFilter {
            MUHEV_FILTER_NONE = 0,
            MUHEV_FILTER_READ = 2,
            MUHEV_FILTER_WRITE = 4
        };

        enum EEvFlags {
            MUHEV_FLAG_NONE = 0,
            MUHEV_FLAG_ONESHOT = 2,
            MUHEV_FLAG_ERROR = 4,
            MUHEV_FLAG_EOF = 8
        };

        struct TEvSpec {
            uintptr_t Ident;
            EEvFilter Filter;
            int Flags;
            void* Ctx;
            intptr_t Data;
        };

        struct TEvList {
            int Count;
            int Pos;
            std::shared_ptr<struct kevent> Events;
            std::shared_ptr<struct kevent> Triggered;
        };

        static inline TEvSpec GetEvent(const TEvList& list, int index) {
            const auto& event = (list.Triggered.get())[index];

            TEvSpec out {
                .Ident = event.ident,
                .Filter = MUHEV_FILTER_NONE,
                .Flags = MUHEV_FLAG_NONE,
                .Ctx = event.udata,
                .Data = event.data
            };

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

            return out;
        }

        static inline TEvList MakeEvList(int count) {
            return TEvList {
                .Count = count,
                .Pos = 0,
                .Events = std::shared_ptr<struct kevent>(
                    new struct kevent[count],
                    std::default_delete<struct kevent[]>()
                ),
                .Triggered = std::shared_ptr<struct kevent>(
                    new struct kevent[count],
                    std::default_delete<struct kevent[]>()
                )
            };
        }

        class TLoop {
        private:
            int QueueId;

        public:
            TLoop() {
                QueueId = kqueue();

                if(QueueId == -1) {
                    perror("kqueue");
                    abort();
                }
            }

            ~TLoop() {
            }

            void AddEvent(const TEvSpec& spec, TEvList& list) {
                int filter = 0;

                switch(spec.Filter) {
                    case MUHEV_FILTER_READ:
                        filter = EVFILT_READ;
                        break;
                    case MUHEV_FILTER_WRITE:
                        filter = EVFILT_WRITE;
                        break;
                    default:
                        throw std::logic_error ("bad filter");
                }

                auto& event = (list.Events.get())[list.Pos++];

                EV_SET(
                    &event,
                    spec.Ident,
                    filter,
                    EV_ADD | EV_ENABLE | (
                        (spec.Flags & MUHEV_FLAG_ONESHOT) ? EV_ONESHOT : 0
                    ),
                    0,
                    0,
                    0
                );

                event.udata = spec.Ctx;
            }

            int Wait(TEvList& list) {
                return kevent(
                    QueueId,
                    list.Events.get(),
                    list.Pos,
                    list.Triggered.get(),
                    list.Pos,
                    nullptr
                );
            }
        };
    }
}
