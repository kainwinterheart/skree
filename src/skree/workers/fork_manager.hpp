#pragma once

#include "../base/worker_lite.hpp"
#include <unistd.h>

namespace Skree {
    namespace Workers {
        class TForkManager : public Skree::Base::WorkerLite {
        public:
            TForkManager(void* _args = nullptr)
                : Skree::Base::WorkerLite(_args) {}

            virtual void run() override;
        };
    }
}

// #include "../server.hpp" // sorry
