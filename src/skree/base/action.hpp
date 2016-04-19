#ifndef _SKREE_BASE_ACTION_H_
#define _SKREE_BASE_ACTION_H_

namespace Skree {
    namespace Base {
        class Action;
    }
    class Server;
    class Client;
}

// #include "../server.hpp"
// #include "../client.hpp"

// #include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

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

            virtual void in(
                const uint64_t& in_len, const char*& in_data,
                uint64_t& out_len, char*& out_data
            ) = 0;
        };
    }
}

#endif
