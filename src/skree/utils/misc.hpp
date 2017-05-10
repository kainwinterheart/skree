#pragma once

#include <functional>
#include <unordered_map>
#include <memory>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <atomic>
#include <execinfo.h>
#include <stdlib.h>

#ifdef SKREE_LONGMESS
#include <cxxabi.h>
#endif

#include <string>
#include <dlfcn.h>
#include <stdarg.h>
#include <unistd.h>
#include <cstddef>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>
#include <algorithm>

#include "string.hpp"
#include "atomic_hash_map.hpp"
#include "events.hpp"

// thank you, stackoverflow!
#ifndef htonll
#define htonll(x) ((1 == htonl(1)) ? (x) : ((uint64_t)htonl(\
    (x) & 0xFFFFFFFF) << 32) | htonl((x) >> 32))
#endif

#ifndef ntohll
#define ntohll(x) ((1 == ntohl(1)) ? (x) : ((uint64_t)ntohl(\
    (x) & 0xFFFFFFFF) << 32) | ntohl((x) >> 32))
#endif

namespace Skree {
    class Client;
    class Server;

    namespace Utils {
        static inline std::shared_ptr<muh_str_t> make_peer_id(
            const size_t peer_name_len,
            const char* peer_name,
            const uint16_t peer_port
        ) {
            auto out = NewStr(
                peer_name_len
                + 1 // :
                + 5 // port string
                + 1 // \0
            );

            memcpy(out->data, peer_name, peer_name_len);
            sprintf(out->data + peer_name_len, ":%u", peer_port);
            out->len = strlen(out->data);

            return out;
        }

        static inline std::shared_ptr<muh_str_t> get_host_from_sockaddr_in(
            const std::shared_ptr<sockaddr_in>& s_in
        ) {
            if(s_in->sin_family == AF_INET) {
                auto out = NewStr(INET_ADDRSTRLEN);
                inet_ntop(AF_INET, &(s_in->sin_addr), out->data, INET_ADDRSTRLEN);
                out->len = strlen(out->data);
                return out;

            } else {
                auto out = NewStr(INET6_ADDRSTRLEN);
                inet_ntop(AF_INET6, &(((sockaddr_in6*)s_in.get())->sin6_addr), out->data, INET6_ADDRSTRLEN);
                out->len = strlen(out->data);
                return out;
            }
        }

        static inline uint16_t get_port_from_sockaddr_in(
            const std::shared_ptr<sockaddr_in>& s_in
        ) {
            if(s_in->sin_family == AF_INET) {
                return ntohs(s_in->sin_port);

            } else {
                return ntohs(((sockaddr_in6*)s_in.get())->sin6_port);
            }
        }

        static inline std::string longmess(int offset = 1) {
            std::string out;

#ifdef SKREE_LONGMESS
            void* callstack[128];
            int frames = backtrace(callstack, 128);
            int status;
            char* name;
            bool dli_ok;
            Dl_info info;
            char tmp [21];
            int starting_offset = offset;

            for(; offset < frames; ++offset) {
                status = dladdr(callstack[offset], &info);

                if((status != 0) && (info.dli_sname != nullptr)) {
                    dli_ok = true;
                    name = abi::__cxa_demangle(info.dli_sname, 0, 0, &status);

                    if(status != 0) {
                        name = strdup(info.dli_sname);
                    }

                } else {
                    if(status != 0)
                        dli_ok = true;

                    name = (char*)malloc(4);
                    sprintf(name, "???");
                    status = 1;
                }

                out.append("\t");

                sprintf(tmp, "%d", (offset - starting_offset));
                out.append(tmp);
                out.append("\t");

                out.append((dli_ok ? info.dli_fname : name));
                out.append("\t");
                out.append(name);

                out.append("\n");

                free(name);
            }
#endif

            return out;
        }

        static inline void cluck(int n, ...) {
            va_list args;
            va_start(args, n);
            auto format = va_arg(args, const char*);

            vfprintf(stderr, format, args);
            va_end(args);

            fprintf(stderr, "\n");
            fprintf(stderr, "%s", longmess(2).c_str());
            fprintf(stderr, "\n");
        }

        static inline bool SetTimeout(int fh, time_t timeoutMilliseconds) {
            timeval tv;
            tv.tv_sec = (timeoutMilliseconds / 1000);
            tv.tv_usec = (timeoutMilliseconds % 1000) * 1000;

            if(setsockopt(fh, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == -1) {
                perror("setsockopt");
                return false;
            }

            if(setsockopt(fh, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == -1) {
                perror("setsockopt");
                return false;
            }

            return true;
        }

        static inline bool SetupSocket(int fh, time_t timeoutMilliseconds) {
            int yes = 1;

            if(setsockopt(fh, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)) == -1) {
                perror("setsockopt");
                return false;
            }

            if(setsockopt(fh, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) == -1) {
                perror("setsockopt");
                return false;
            }

            return Utils::SetTimeout(fh, timeoutMilliseconds);
        }

        template<typename TBuf>
        static inline void PrintString(TBuf buf, const size_t size) {
            for(size_t i = 0; i < size; ++i) {
                fprintf(stderr, "[%zu] 0x%X\n", i, ((unsigned char*)buf)[i]);
            }
        }

        static inline uint64_t ThreadId() {
            pthread_t ptid = pthread_self();
            uint64_t threadId = 0;
            memcpy(&threadId, &ptid, std::min(sizeof(threadId), sizeof(ptid)));
            return threadId;
        }
    }
}
