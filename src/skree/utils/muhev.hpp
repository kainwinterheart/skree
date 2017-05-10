#pragma once

#include <memory>

#ifdef __linux__
#include <unordered_map>
struct epoll_event;
#else
struct kevent;
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
        };

#ifdef __linux__
        using TInternalEvStruct = struct ::epoll_event;
#else
        using TInternalEvStruct = struct ::kevent;
#endif

        struct TEvList {
            int Count;
            int Pos;
            std::shared_ptr<TInternalEvStruct> Events;
            std::shared_ptr<TInternalEvStruct> Triggered;
#ifdef __linux__
            std::unordered_map<uintptr_t, void*> FdMap;
#endif
        };

        extern TEvSpec GetEvent(const TEvList& list, int index);
        extern TEvList MakeEvList(int count);

        class TLoop {
        private:
            int QueueId;

        public:
            TLoop();
            ~TLoop();

        public:
            void AddEvent(const TEvSpec& spec, TEvList& list);
            int Wait(TEvList& list);
        };
    }
}
