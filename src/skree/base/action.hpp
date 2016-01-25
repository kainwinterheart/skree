#ifndef _SKREE_BASE_ACTION_H_
#define _SKREE_BASE_ACTION_H_

#include "../server.hpp"
#include "../client.hpp"

namespace Skree {
    namespace Base {
        class Action {
        protected:
            Skree::Server* server;
            Skree::Client* client;
        public:
            Action(
                Skree::Server* _server,
                Skree::Client* _client
            ) : server(_server), client(_client) {}

            virtual char opcode();
            virtual void in(
                uint64_t in_len, char* in_data,
                uint64_t* out_len, char** out_data
            );
        };
    }
}

#endif
