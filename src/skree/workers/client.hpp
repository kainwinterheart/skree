#pragma once
#include "../base/worker.hpp"

namespace Skree {
    namespace Workers {
        class Client : public Skree::Base::Worker {
        public:
            Client(Skree::Server& _server, const void* _args = nullptr)
                : Skree::Base::Worker(_server, _args) {}

            virtual void run() override;
        };
    }
}

#include "../server.hpp" // sorry

