#ifndef _SKREE_BASE_WORKER_H_
#define _SKREE_BASE_WORKER_H_

namespace Skree {
    class Server;
}

// #include "../server.hpp"
// #include "../client.hpp"
#include <stdlib.h>
#include <pthread.h>

namespace Skree {
    namespace Base {
        class Worker {
        protected:
            Skree::Server& server;
            const void* args;
        public:
            Worker(Skree::Server& _server, const void* _args = NULL)
                : server(_server), args(_args) {
                thread = (pthread_t*)malloc(sizeof(*thread));
                pthread_create(thread, NULL, __run, (void*)this);
            }

            virtual ~Worker() {
                pthread_join(*thread, NULL);
                free(thread);
            }

            virtual void run() = 0;
        private:
            pthread_t* thread;

            static void* __run(void* args) {
                ((Worker*)args)->run();
                return NULL;
            }
        };
    }
}

#endif
