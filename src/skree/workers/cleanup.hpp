#pragma once

#include "../base/worker.hpp"
#include <unistd.h>

namespace Skree {
    class QueueDb;

    namespace Workers {
        class Cleanup : public Skree::Base::Worker {
        public:
            Cleanup(Skree::Server& _server, const void* _args = nullptr)
                : Skree::Base::Worker(_server, _args) {}

            virtual void run() override;
        private:
            void cleanup_queue(const Skree::QueueDb& queue) const;
        };
    }
}

// #include "../server.hpp" // sorry
