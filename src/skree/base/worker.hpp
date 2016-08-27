#pragma once

namespace Skree {
    class Server;
}

#include "worker_lite.hpp"
#include <stdlib.h>
#include <pthread.h>
#include <functional>

namespace Skree {
    namespace Base {
        class Worker : public Skree::Base::WorkerLite {
        protected:
            Skree::Server& server;
        public:
            Worker(Skree::Server& _server, const void* _args = nullptr)
                : Skree::Base::WorkerLite(_args), server(_server) {
            }

            virtual ~Worker() {
            }
        };
    }
}
