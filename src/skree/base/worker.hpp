#ifndef _SKREE_BASE_WORKER_H_
#define _SKREE_BASE_WORKER_H_

namespace Skree {
    class Server;
}

// #include "../server.hpp"
// #include "../client.hpp"
#include <stdlib.h>
#include <pthread.h>
#include <functional>

namespace Skree {
    namespace Base {
        class Worker {
        protected:
            Skree::Server& server;
            const void* args;
        public:
            Worker(Skree::Server& _server, const void* _args = nullptr)
                : server(_server), args(_args) {
            }

            virtual void start() {
                thread = (pthread_t*)malloc(sizeof(*thread));

                run_args* args = new run_args {
                    .cb = [this](){
                        run();
                    }
                };

                pthread_create(thread, nullptr, __run, (void*)args);
            }

            virtual ~Worker() {
                if(thread != nullptr) {
                    pthread_join(*thread, nullptr);
                    free(thread);
                }
            }

            virtual void run() = 0;
        private:
            pthread_t* thread;
            struct run_args {
                std::function<void()> cb;
            };

            static void* __run(void* args) {
                ((run_args*)args)->cb();
                free(args);
                return nullptr;
            }
        };
    }
}

#endif
