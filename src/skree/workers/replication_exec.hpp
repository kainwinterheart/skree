#ifndef _SKREE_WORKERS_REPLICATIONEXEC_H_
#define _SKREE_WORKERS_REPLICATIONEXEC_H_

#include "../base/worker.hpp"

namespace Skree {
    namespace Workers {
        class ReplicationExec : public Skree::Base::Worker {
        public:
            ReplicationExec(Skree::Server& _server, const void* _args = NULL)
                : Skree::Base::Worker(_server, _args) {}

            virtual void run() override;
        };
    }
}

#include "../server.hpp" // sorry

#endif
