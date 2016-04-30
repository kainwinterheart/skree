#ifndef _SKREE_BASE_PENDINGREAD_H_
#define _SKREE_BASE_PENDINGREAD_H_

namespace Skree {
    namespace Base {
        namespace PendingRead {
            struct QueueItem;
            class Callback;
        }
    }
    class Server;
    class Client;
}

// #include "../server.hpp"
// #include "../client.hpp"
#include "pending_write.hpp"
#include <sys/types.h>
#include <stdexcept>

namespace Skree {
    namespace Base {
        namespace PendingRead {
            class Callback {
            protected:
                Skree::Server& server;
            public:
                struct Args {
                    size_t& out_len;
                    const char*& data;
                    char*& out_data;
                    bool& stop;
                };

                Callback(Skree::Server& _server) : server(_server) {}

                virtual Skree::Base::PendingWrite::QueueItem* run(
                    Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item,
                    Args& args
                ) = 0;

                virtual void error(
                    Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item
                ) {}

                virtual bool noop() { return false; };
            };

            struct QueueItem {
                size_t len;
                Callback* cb;
                void* ctx;
                bool opcode;
                bool noop;
            };
        }
    }
}

#endif
