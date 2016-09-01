#include "w.hpp"

namespace Skree {
    namespace Actions {
        void W::in(
            const uint64_t in_len, const char* in_data,
            std::shared_ptr<Skree::Base::PendingWrite::QueueItem>& out
        ) {
            out.reset(new Skree::Base::PendingWrite::QueueItem (SKREE_META_OPCODE_K));

            uint32_t _hostname_len = htonl(server.my_hostname_len);
            out->copy_concat(sizeof(_hostname_len), &_hostname_len);
            out->concat(server.my_hostname_len + 1, server.my_hostname);

            const uint32_t max_parallel_connections (htonl(server.get_max_parallel_connections()));
            out->copy_concat(sizeof(max_parallel_connections), &max_parallel_connections);
            // Utils::cluck(1, "W::in done\n");
        }

        std::shared_ptr<Skree::Base::PendingWrite::QueueItem> W::out_init() {
            return std::make_shared<Skree::Base::PendingWrite::QueueItem>(opcode());
        }
    }
}
