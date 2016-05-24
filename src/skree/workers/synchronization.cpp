#include "synchronization.hpp"

namespace Skree {
    namespace Workers {
        void Synchronization::run() {
            while(true) {
                sleep(1);

                uint_fast64_t stat_num_inserts = server.stat_num_inserts;
                uint_fast64_t stat_num_replications = server.stat_num_replications;
                uint_fast64_t stat_num_repl_it = server.stat_num_repl_it;
                uint_fast64_t stat_num_proc_it = server.stat_num_proc_it;

                if(stat_num_inserts > 0)
                    printf("number of inserts for last second: %llu\n",
                        stat_num_inserts);

                if(stat_num_replications > 0)
                    printf("number of replication inserts for last second: %llu\n",
                        stat_num_replications);

                if(stat_num_repl_it > 0)
                    printf("number of replication iterations for last second: %llu\n",
                        stat_num_repl_it);

                if(stat_num_proc_it > 0)
                    printf("number of processor iterations for last second: %llu\n",
                        stat_num_proc_it);

                server.stat_num_inserts -= stat_num_inserts;
                server.stat_num_replications -= stat_num_replications;
                server.stat_num_repl_it -= stat_num_repl_it;
                server.stat_num_proc_it -= stat_num_proc_it;
            }
        }
    }
}
