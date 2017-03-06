#include "statistics.hpp"

namespace Skree {
    namespace Workers {
        void Statistics::run() {
            while(true) {
                sleep(1);

                uint_fast64_t stat_num_inserts = server.stat_num_inserts;
                uint_fast64_t stat_num_replications = server.stat_num_replications;
                uint_fast64_t stat_num_repl_it = server.stat_num_repl_it;

                if(stat_num_inserts > 0) {
                    fprintf(stderr, "inserts: %llu\n", stat_num_inserts);
                    server.stat_num_inserts -= stat_num_inserts;
                }

                if(stat_num_replications > 0) {
                    fprintf(stderr, "replication inserts: %llu\n", stat_num_replications);
                    server.stat_num_replications -= stat_num_replications;
                }

                if(stat_num_repl_it > 0) {
                    fprintf(stderr, "replication iterations: %llu\n", stat_num_repl_it);
                    server.stat_num_repl_it -= stat_num_repl_it;
                }

                for(auto& it : server.known_events) {
                    auto& event = *(it.second);

                    uint_fast64_t max = 0;
                    uint_fast64_t value = event.stat_num_processed;

                    if(value > 0) {
                        fprintf(stderr, "\t[%s] processed: %llu\n", event.id, value);
                        event.stat_num_processed -= value;
                        max = value;
                    }

                    value = event.stat_num_failovered;

                    if(value > 0) {
                        fprintf(stderr, "\t[%s] failovered: %llu\n", event.id, value);
                        event.stat_num_failovered -= value;

                        if(value > max) {
                            max = value;
                        }
                    }

                    if(max > 0) {
                        fprintf(stderr, "processor iterations: %llu\n", max);
                    }
                }

                {
                    uint_fast64_t stat_num_requests = 0;

                    for(unsigned char i = 0; i <= 255;) {
                        uint_fast64_t value = server.stat_num_requests_detailed[i];

                        if(value > 0) {
                            stat_num_requests += value;
                            server.stat_num_requests_detailed[i] -= value;
                            fprintf(stderr, "\t\"%c\" requests: %llu\n", i, value);
                        }

                        if(i < 255) {
                            ++i;

                        } else if(i == 255) {
                            break;
                        }
                    }

                    if(stat_num_requests > 0) {
                        fprintf(stderr, "requests total: %llu\n", stat_num_requests);
                    }
                }

                {
                    uint_fast64_t stat_num_responses = 0;

                    for(unsigned char i = 0; i <= 255;) {
                        uint_fast64_t value = server.stat_num_responses_detailed[i];

                        if(value > 0) {
                            stat_num_responses += value;
                            server.stat_num_responses_detailed[i] -= value;
                            fprintf(stderr, "\t\"%c\" responses: %llu\n", i, value);
                        }

                        if(i < 255) {
                            ++i;

                        } else if(i == 255) {
                            break;
                        }
                    }

                    if(stat_num_responses > 0) {
                        fprintf(stderr, "responses total: %llu\n", stat_num_responses);
                    }
                }
            }
        }
    }
}
