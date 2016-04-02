#ifndef _SKREE_PENDINGREADS_REPLICATION_PROPOSESELF_H_
#define _SKREE_PENDINGREADS_REPLICATION_PROPOSESELF_H_

#include "../../base/pending_read.hpp"

namespace Skree {
    namespace PendingReads {
        namespace Callbacks {
            namespace Replication {
                class ProposeSelf : public Skree::Base::PendingRead::Callback {
                private:
                    void continue_replication_exec(out_packet_i_ctx*& ctx);
                }
            }
        }
    }
}

#endif
