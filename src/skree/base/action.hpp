#pragma once
namespace Skree {
    namespace Base {
        class Action;
        namespace PendingWrite {
            class QueueItem;
        }
    }
    class Server;
    class Client;
}

#include "pending_read.hpp"
// #include "../server.hpp"
// #include "../client.hpp"

// #include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <cstdint>
#include <stdio.h>
#include <memory>

namespace Skree {
    namespace Base {
        class Action {
        protected:
            Skree::Server& server;
            Skree::Client& client;
        public:
            Action(
                Skree::Server& _server,
                Skree::Client& _client
            ) : server(_server), client(_client) {}

            static const char opcode();

            virtual void in(std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args) = 0;
        };
    }
}
