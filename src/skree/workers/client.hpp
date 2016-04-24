#ifndef _SKREE_WORKERS_CLIENT_H_
#define _SKREE_WORKERS_CLIENT_H_

#include "../base/worker.hpp"

namespace Skree {
    namespace Workers {
        class Client : public Skree::Base::Worker {
        public:
            Client(Skree::Server& _server, const void* _args = NULL)
                : Skree::Base::Worker(_server, _args) {}

            virtual void run() override;
        };
    }
}

#include "../server.hpp" // sorry

#endif
