#include "w.hpp"

namespace Skree {
    namespace Actions {
        void W::in(
            uint64_t in_len, char* in_data,
            uint64_t* out_len, char** out_data
        ) {
            char* _out_data = (char*)malloc(1
                + sizeof(server->my_hostname_len)
                + server->my_hostname_len);

            _out_data[0] = SKREE_META_OPCODE_K;
            *out_len += 1;

            uint32_t _hostname_len = htonl(server->my_hostname_len);
            memcpy(_out_data + *out_len, (char*)&_hostname_len, sizeof(_hostname_len));
            *out_len += sizeof(_hostname_len);

            memcpy(_out_data + *out_len, server->my_hostname, server->my_hostname_len);
            *out_len += server->my_hostname_len;

            *out_data = _out_data;
        }

        static muh_str_t* W::out_init() {
            muh_str_t* out = (muh_str_t*)malloc(sizeof(*out));
            out->len = 1;
            out->data = (char*)malloc(1);
            out->data[0] = opcode();
            return out;
        }
    }
}
