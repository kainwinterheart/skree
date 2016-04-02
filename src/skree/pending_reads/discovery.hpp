#ifndef _SKREE_PENDINGREADS_REPLICATION_H_
#define _SKREE_PENDINGREADS_REPLICATION_H_

#include "../base/pending_read.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            template<typename F>
            class Discovery : public Skree::Base::PendingRead::Callback {
            private:
                const F&& cb;
            public:
                Discovery(const Skree::Server& _server, const F& _cb)
                : Skree::Base::PendingRead::Callback(_server) {
                    cb = std::move(_cb);
                }
            }
        }
    }
}

#endif
