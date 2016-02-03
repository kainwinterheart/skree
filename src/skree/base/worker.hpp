#ifndef _SKREE_BASE_WORKER_H_
#define _SKREE_BASE_WORKER_H_

#include "../server.hpp"
// #include "../client.hpp"

namespace Skree {
    namespace Base {
        class Worker {
        protected:
            Skree::Server* server;
            const void* args;
        public:
            Action(Skree::Server* _server, const void* _args = NULL)
                : server(_server), args(_args) {
                thread = (pthread_t*)malloc(sizeof(*thread));
                pthread_create(thread, NULL, __run, (void*)this);
            }

            virtual ~Action() { pthread_join(thread); }
            virtual void run();
        private:
            pthread_t* thread;

            static *void __run(void* args) {
                ((Worker*)args)->run();
                return NULL;
            }
        };
    }
}

#endif
