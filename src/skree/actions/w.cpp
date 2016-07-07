#include "w.hpp"

namespace Skree {
    namespace Actions {
        void W::in(
            const uint64_t& in_len, const char*& in_data,
            uint64_t& out_len, char*& out_data
        ) {
            out_data = (char*)malloc(1
                + sizeof(server.my_hostname_len)
                + server.my_hostname_len
                + sizeof(uint32_t)
            );

            out_data[0] = SKREE_META_OPCODE_K;
            out_len += 1;

            uint32_t _hostname_len = htonl(server.my_hostname_len);
            memcpy(out_data + out_len, (char*)&_hostname_len, sizeof(_hostname_len));
            out_len += sizeof(_hostname_len);

            memcpy(out_data + out_len, server.my_hostname, server.my_hostname_len);
            out_len += server.my_hostname_len;

            const uint32_t max_parallel_connections (htonl(server.get_max_parallel_connections()));
            memcpy(out_data + out_len, &max_parallel_connections, sizeof(max_parallel_connections));
            out_len += sizeof(max_parallel_connections);
            // printf("W::in done\n");
        }

        Utils::muh_str_t* W::out_init() {
            Utils::muh_str_t* out = (Utils::muh_str_t*)malloc(sizeof(*out));
            out->len = 1;
            out->data = (char*)malloc(1);
            out->data[0] = opcode();
            return out;
        }
    }
}
