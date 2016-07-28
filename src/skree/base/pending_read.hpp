#pragma once
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
                    const char*& data;
                    Skree::Base::PendingWrite::QueueItem*& out;
                    bool& stop;
                    const char opcode;
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
            };

            struct QueueItem {
                size_t len;
                Callback* cb;
                void* ctx;
                bool opcode;
            };
        }
    }
}
