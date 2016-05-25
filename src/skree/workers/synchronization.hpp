#pragma once

#include "../base/worker.hpp"
#include <unistd.h>

namespace Skree {
    namespace Workers {
        class Synchronization : public Skree::Base::Worker {
        public:
            Synchronization(Skree::Server& _server, const void* _args = nullptr)
                : Skree::Base::Worker(_server, _args) {}

            virtual void run() override;
        };
    }
}

#include "../server.hpp" // sorry

