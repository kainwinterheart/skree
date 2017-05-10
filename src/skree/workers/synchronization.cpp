#include "synchronization.hpp"
#include "../queue_db.hpp"
#include "../server.hpp"

namespace Skree {
    namespace Workers {
        void Synchronization::run() {
            while(true) {
                sleep(1);

                for(const auto& it : server.known_events) {
                    const auto& event = *(it.second);

                    event.queue->sync();
                    event.queue2->sync();
                    event.r_queue->sync();
                    event.r2_queue->sync();
                }
            }
        }
    }
}
