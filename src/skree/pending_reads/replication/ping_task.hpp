#ifndef _SKREE_PENDINGREADS_REPLICATION_PROPOSESELF_H_
#define _SKREE_PENDINGREADS_REPLICATION_PROPOSESELF_H_

// #include "../../base/pending_read.hpp"

namespace Skree {
    struct out_data_c_ctx {
        known_event_t* event;
        muh_str_t* rin;
        muh_str_t* rpr;
        uint64_t rid;
        uint64_t wrinseq;
        uint64_t failover_key_len;
        char* failover_key;
    };

    namespace PendingReads {
        namespace Callbacks {
            namespace Replication {
                class PingTask : public Skree::Base::PendingRead::Callback {
                }
            }
        }
    }
}

#endif
