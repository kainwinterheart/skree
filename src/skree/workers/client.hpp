#pragma once

#include "../base/worker.hpp"
#include "../utils/misc.hpp"

#include <ev.h>
#include <queue>

namespace Skree {
    struct new_client_t;

    namespace Workers {
        class Client : public Skree::Base::Worker {
        public:
            struct bound_ev_async {
                ev_async watcher;
                Client* worker;
            };

            struct Args {
                struct ev_loop* loop;
                bound_ev_async* watcher;
                std::queue<new_client_t*>* queue;
                pthread_mutex_t* mutex;
            };

            Client(Skree::Server& _server, const void* _args = nullptr)
                : Skree::Base::Worker(_server, _args)
            {
                ((Args*)args)->watcher->worker = this;
                ev_async_init((ev_async*)(((Args*)args)->watcher), async_cb);
                ev_async_start(((Args*)args)->loop, (ev_async*)(((Args*)args)->watcher));
            }

            virtual void run() override;
            static void async_cb(struct ev_loop* loop, ev_async* _watcher, int events);
            void accept();

            virtual ~Client() {
            }
        };
    }
}

#include "../server.hpp" // sorry
