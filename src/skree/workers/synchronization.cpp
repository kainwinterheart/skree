#include "synchronization.hpp"

namespace Skree {
    namespace Workers {
        void Synchronization::run() {
            while(true) {
                sleep(1);
                server.db.synchronize();

                pthread_mutex_lock(&(server.stat_mutex));

                if(server.stat_num_inserts > 0)
                    printf("number of inserts for last second: %lu\n",
                        server.stat_num_inserts);

                if(server.stat_num_replications > 0)
                    printf("number of replication inserts for last second: %lu\n",
                        server.stat_num_replications);

                server.stat_num_inserts = 0;
                server.stat_num_replications = 0;

                pthread_mutex_unlock(&(server.stat_mutex));
            }
        }
    }
}
