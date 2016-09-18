#pragma once
#include <ev.h>

#include <functional>
#include <unordered_map>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <atomic>
#include <execinfo.h>
#include <stdlib.h>
#include <cxxabi.h>
#include <string>
#include <dlfcn.h>
#include <stdarg.h>
#include <unistd.h>

#include "atomic_hash_map.hpp"

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
    class QueueDb;

    namespace Utils {
        struct client_bound_ev_io {
            ev_io watcher;
            Client* client;
        };

        struct server_bound_ev_io {
            ev_io watcher;
            Server* server;
        };

        struct muh_str_t {
            char* data;
            uint32_t len;
            bool own;

            ~muh_str_t() {
                if(own)
                    free(data);
            }
        };

        struct TCharPointerComparator : public std::function<bool(
            const char*, const char*
        )> {
            inline bool operator()(const char* a, const char* b) const {
                return (strcmp(a, b) == 0);
            }
        };

        struct TMuhStrPointerComparator : public std::function<bool(
            std::shared_ptr<muh_str_t>, std::shared_ptr<muh_str_t>
        )> {
            inline bool operator()(
                const std::shared_ptr<muh_str_t>& a,
                const std::shared_ptr<muh_str_t>& b
            ) const {
                return (strcmp(a->data, b->data) == 0);
            }
        };

        //BKDR hash algorithm
        static inline int CharPointerHash(size_t len, const char* key) {
            const int seed = 131; //31 131 1313 13131131313 etc//
            int hash = 0;

            for(size_t i = 0; i < len; ++i) {
                hash = ((hash * seed) + key[i]);
            }

            return (hash & 0x7FFFFFFF);
        }

        struct TCharPointerHasher {
            inline int operator()(const char* key) const {
                return CharPointerHash(strlen(key), key);
            }
        };

        struct TMuhStrPointerHasher {
            inline int operator()(const std::shared_ptr<muh_str_t>& key) const {
                return CharPointerHash(key->len, key->data);
            }
        };

        typedef AtomicHashMap<
            std::shared_ptr<muh_str_t>,
            uint64_t,
            Utils::TMuhStrPointerHasher,
            Utils::TMuhStrPointerComparator
        > failover_t;

        typedef AtomicHashMap<
            std::shared_ptr<muh_str_t>,
            uint64_t,
            Utils::TMuhStrPointerHasher,
            Utils::TMuhStrPointerComparator
        > no_failover_t;

        struct skree_module_t {
            size_t path_len;
            char* path;
            const void* config;
        };

        struct event_group_t {
            size_t name_len;
            char* name;
            skree_module_t* module;
        };

        struct known_event_t {
            uint32_t id_len;
            uint32_t id_len_net;
            char* id;
            event_group_t* group;
            uint32_t ttl;
            QueueDb* queue;
            QueueDb* queue2;
            QueueDb* r_queue;
            QueueDb* r2_queue;
            std::atomic<uint_fast64_t> stat_num_processed;
            std::atomic<uint_fast64_t> stat_num_failovered;
            failover_t failover;
            no_failover_t no_failover;

            void unfailover(const std::shared_ptr<muh_str_t>& failover_key) {
                {
                    auto failover_end = failover.lock();
                    auto it = failover.find(failover_key);

                    if(it != failover_end)
                        failover.erase(it);

                    failover.unlock();
                }

                {
                    auto no_failover_end = no_failover.lock();
                    auto it = no_failover.find(failover_key);

                    if(it != no_failover_end)
                        no_failover.erase(it);

                    no_failover.unlock();
                }
            }
        };

        typedef std::unordered_map<
            const char*,
            skree_module_t*,
            TCharPointerHasher,
            TCharPointerComparator
        > skree_modules_t;

        typedef std::unordered_map<
            const char*,
            event_group_t*,
            TCharPointerHasher,
            TCharPointerComparator
        > event_groups_t;

        typedef std::unordered_map<
            const char*,
            known_event_t*,
            TCharPointerHasher,
            TCharPointerComparator
        > known_events_t;

        static inline std::shared_ptr<muh_str_t> NewStr(uint32_t len) {
            auto out = std::shared_ptr<muh_str_t>();
            out.reset(new muh_str_t {
                .own = true,
                .len = len,
                .data = (char*)malloc(len)
            });

            return out;
        }

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

        static inline size_t write_chunk(int fh, size_t size, void* data) {
            size_t written;
            size_t total = 0;

            while(total < size) {
                written = ::write(fh, ((unsigned char*)data) + total, size - total);

                if(written == -1) {
                    perror("write");
                    break;

                } else {
                    total += written;
                }
            }

            return total;
        }

        static inline std::string longmess(int offset = 1) {
            std::string out;

            // void* callstack[128];
            // int frames = backtrace(callstack, 128);
            // int status;
            // char* name;
            // bool dli_ok;
            // Dl_info info;
            // char tmp [21];
            // int starting_offset = offset;
            //
            // for(; offset < frames; ++offset) {
            //     status = dladdr(callstack[offset], &info);
            //
            //     if((status != 0) && (info.dli_sname != nullptr)) {
            //         dli_ok = true;
            //         name = abi::__cxa_demangle(info.dli_sname, 0, 0, &status);
            //
            //         if(status != 0) {
            //             name = strdup(info.dli_sname);
            //         }
            //
            //     } else {
            //         if(status != 0)
            //             dli_ok = true;
            //
            //         name = (char*)malloc(4);
            //         sprintf(name, "???");
            //         status = 1;
            //     }
            //
            //     out.append("\t");
            //
            //     sprintf(tmp, "%d", (offset - starting_offset));
            //     out.append(tmp);
            //     out.append("\t");
            //
            //     out.append((dli_ok ? info.dli_fname : name));
            //     out.append("\t");
            //     out.append(name);
            //
            //     out.append("\n");
            //
            //     free(name);
            // }

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
    }
}
