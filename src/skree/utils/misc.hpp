#ifndef _SKREE_UTILS_MISC_H_
#define _SKREE_UTILS_MISC_H_

namespace Skree {
    namespace Utils {
        static inline char* make_peer_id(size_t peer_name_len, char* peer_name, uint32_t peer_port) {
            char* peer_id = (char*)malloc(peer_name_len
                + 1 // :
                + 5 // port string
                + 1 // \0
            );

            memcpy(peer_id, peer_name, peer_name_len);
            sprintf(peer_id + peer_name_len, ":%u", peer_port);

            return peer_id;
        }

        static inline char* get_host_from_sockaddr_in(const sockaddr_in* s_in) {
            char* conn_name = NULL;

            if(s_in->sin_family == AF_INET) {
                conn_name = (char*)malloc(INET_ADDRSTRLEN);
                inet_ntop(AF_INET, &(s_in->sin_addr), conn_name, INET_ADDRSTRLEN);

            } else {
                conn_name = (char*)malloc(INET6_ADDRSTRLEN);
                inet_ntop(AF_INET6, &(((sockaddr_in6*)s_in)->sin6_addr), conn_name, INET6_ADDRSTRLEN);
            }

            return conn_name;
        }

        static inline uint32_t get_port_from_sockaddr_in(const sockaddr_in* s_in) {
            if(s_in->sin_family == AF_INET) {
                return ntohs(s_in->sin_port);

            } else {
                return ntohs(((sockaddr_in6*)s_in)->sin6_port);
            }
        }
    }
}

#endif
