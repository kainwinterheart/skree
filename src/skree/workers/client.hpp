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
                std::shared_ptr<bound_ev_async> watcher;
                std::shared_ptr<std::queue<std::shared_ptr<new_client_t>>> queue;
                std::shared_ptr<pthread_mutex_t> mutex;

                Args()
                    : loop(ev_loop_new(EVBACKEND_KQUEUE | EVBACKEND_EPOLL | EVFLAG_NOSIGMASK))
                    , watcher(std::make_shared<bound_ev_async>())
                    , queue(std::make_shared<std::queue<std::shared_ptr<new_client_t>>>())
                    , mutex(std::make_shared<pthread_mutex_t>())
                {
                    pthread_mutex_init(mutex.get(), NULL);
                }

                ~Args() {
                    pthread_mutex_destroy(mutex.get());
                }
            };

            Client(Skree::Server& _server, const void* _args = nullptr)
                : Skree::Base::Worker(_server, _args)
            {
                ((Args*)args)->watcher->worker = this;
                ev_async_init((ev_async*)(((Args*)args)->watcher.get()), async_cb);
                ev_async_start(((Args*)args)->loop, (ev_async*)(((Args*)args)->watcher.get()));
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
