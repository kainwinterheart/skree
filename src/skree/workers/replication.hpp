#pragma once

#include "../base/worker.hpp"
#include "../utils/misc.hpp"
#include "../pending_reads/replication/propose_self.hpp"
#include "../pending_reads/replication/ping_task.hpp"

#include <vector>
#include <string>

namespace Skree {
    namespace Workers {
        class Replication : public Skree::Base::Worker {
        public:
            Replication(Skree::Server& _server, const void* _args = nullptr)
                : Skree::Base::Worker(_server, _args) {}

            virtual void run() override;
        };
    }
}

#include "../server.hpp" // sorry
