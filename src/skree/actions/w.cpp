#include "w.hpp"

namespace Skree {
    namespace Actions {
        void W::in(
            const uint64_t in_len, const char* in_data,
            Skree::Base::PendingWrite::QueueItem*& out
        ) {
            out = new Skree::Base::PendingWrite::QueueItem ((
                sizeof(server.my_hostname_len)
                + server.my_hostname_len
                + sizeof(uint32_t)
            ), SKREE_META_OPCODE_K);

            uint32_t _hostname_len = htonl(server.my_hostname_len);
            out->push(sizeof(_hostname_len), (char*)&_hostname_len);
            out->push(server.my_hostname_len, server.my_hostname);

            const uint32_t max_parallel_connections (htonl(server.get_max_parallel_connections()));
            out->push(sizeof(max_parallel_connections), &max_parallel_connections);
            // Utils::cluck(1, "W::in done\n");
        }

        Skree::Base::PendingWrite::QueueItem* W::out_init() {
            return new Skree::Base::PendingWrite::QueueItem(0, opcode());
        }
    }
}
