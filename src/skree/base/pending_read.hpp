#ifndef _SKREE_BASE_PENDINGREAD_H_
#define _SKREE_BASE_PENDINGREAD_H_

#include "../server.hpp"
#include "../client.hpp"

namespace Skree {
    namespace Base {
        namespace PendingRead {
            struct QueueItem {
                size_t len;
                const Callback&& cb;
                void*& ctx;
                bool opcode;
                bool noop;
            };

            class Callback {
            protected:
                const Skree::Server& server;
            public:
                struct Args {
                    size_t& out_len;
                    const char*& data;
                    char*& out_data;
                    bool& stop;
                };

                Callback(const Skree::Server& _server) : server(_server) {}

                virtual const QueueItem&& run(
                    const Skree::Client& client,
                    const QueueItem& item,
                    const Args& args
                );

                virtual void error(
                    const Skree::Client& client,
                    const QueueItem& item
                ) {}

                virtual bool noop() { return false; };
            };
        }
    }
}

#endif
