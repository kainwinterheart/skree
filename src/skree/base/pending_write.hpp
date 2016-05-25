#pragma once
namespace Skree {
    namespace Base {
        namespace PendingWrite {
            struct QueueItem;
        }
    }
}

// #include "../server.hpp"
// #include "../client.hpp"
#include <stdlib.h>
#include "pending_read.hpp"

namespace Skree {
    namespace Base {
        namespace PendingWrite {
            struct QueueItem {
                size_t len;
                size_t pos;
                char* data;
                const Skree::Base::PendingRead::QueueItem* cb;
            };
        }
    }
}

