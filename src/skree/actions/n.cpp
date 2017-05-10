#include "n.hpp"
#include "../client.hpp"

namespace Skree {
    namespace Actions {
        void N::in(std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args) {
            uint32_t version;
            memcpy(&version, args->data, sizeof(version));

            client.set_protocol_version(ntohl(version));
        }

        std::shared_ptr<Skree::Base::PendingWrite::QueueItem> N::out_init() {
            const uint32_t protocol_version (htonl(PROTOCOL_VERSION));
            auto out = std::make_shared<Skree::Base::PendingWrite::QueueItem>(opcode());

            out->copy_concat(sizeof(protocol_version), &protocol_version);
            out->finish();

            return out;
        }
    }
}
