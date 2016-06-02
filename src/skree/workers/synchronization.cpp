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

                if(stat_num_inserts > 0) {
                    printf("inserts: %llu\n", stat_num_inserts);
                    server.stat_num_inserts -= stat_num_inserts;
                }

                if(stat_num_replications > 0) {
                    printf("replication inserts: %llu\n", stat_num_replications);
                    server.stat_num_replications -= stat_num_replications;
                }

                if(stat_num_repl_it > 0) {
                    printf("replication iterations: %llu\n", stat_num_repl_it);
                    server.stat_num_repl_it -= stat_num_repl_it;
                }

                if(stat_num_proc_it > 0) {
                    printf("processor iterations: %llu\n", stat_num_proc_it);
                    server.stat_num_proc_it -= stat_num_proc_it;
                }

                uint_fast64_t stat_num_requests = 0;

                for(unsigned char i = 0; i <= 255; ++i) {
                    uint_fast64_t value = server.stat_num_requests_detailed[i];

                    if(value > 0) {
                        stat_num_requests += value;
                        server.stat_num_requests_detailed[i] -= value;
                        printf("\t\"%c\" requests: %llu\n", i, value);
                    }
                }

                // if(stat_num_requests > 0) {
                    printf("requests total: %llu\n", stat_num_requests);
                // }
            }
        }
    }
}
