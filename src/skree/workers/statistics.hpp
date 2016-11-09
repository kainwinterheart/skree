#pragma once

#include "../base/worker.hpp"
#include <unistd.h>

namespace Skree {
    class QueueDb;

    namespace Workers {
        class Statistics : public Skree::Base::Worker {
        public:
            Statistics(Skree::Server& _server, const void* _args = nullptr)
                : Skree::Base::Worker(_server, _args) {}

            virtual void run() override;
        };
    }
}

#include "../server.hpp" // sorry
