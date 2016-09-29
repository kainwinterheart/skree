#pragma once

#include "../base/worker.hpp"
#include "../utils/misc.hpp"

#include <ev.h>
#include <queue>
#include <deque>

#include <sys/types.h>
#include <sys/event.h>
// #include <sys/time.h>

namespace Skree {
    struct new_client_t;

    namespace Workers {
        class Client : public Skree::Base::Worker {
        public:
            // struct bound_ev_io {
            //     ev_io watcher;
            //     Client* worker;
            // };

            struct Args {
                // struct ev_loop* loop;
                // std::shared_ptr<bound_ev_io> watcher;
                std::shared_ptr<std::queue<std::shared_ptr<new_client_t>>> queue;
                std::shared_ptr<pthread_mutex_t> mutex;
                int fds[2];

                Args()
                    // : loop(ev_loop_new(EVBACKEND_KQUEUE | EVBACKEND_EPOLL | EVFLAG_NOSIGMASK))
                    // , watcher(std::make_shared<bound_ev_io>())
                    : queue(std::make_shared<std::queue<std::shared_ptr<new_client_t>>>())
                    , mutex(std::make_shared<pthread_mutex_t>())
                {
                    pthread_mutex_init(mutex.get(), NULL);

                    // if(socketpair(PF_LOCAL, SOCK_STREAM, 0, fds) == -1) {
                    if(pipe(fds) == -1) {
                        perror("pipe");
                        abort();
                    }
                }

                ~Args() {
                    pthread_mutex_destroy(mutex.get());

                    for(int i = 0; i < 2; ++i) {
                        close(fds[i]);
                    }
                }
            };

            Client(Skree::Server& _server, const void* _args = nullptr)
                : Skree::Base::Worker(_server, _args)
            {
                // ((Args*)args)->watcher->worker = this;
                // ev_io_init((ev_io*)(((Args*)args)->watcher.get()), async_cb, ((Args*)args)->fds[0], EV_READ);
                // ev_io_start(((Args*)args)->loop, (ev_io*)(((Args*)args)->watcher.get()));
            }

            virtual void run() override;
            static void async_cb(struct ev_loop* loop, ev_io* _watcher, int events);
            void accept();

            virtual ~Client() {
            }

        private:
            std::deque<std::shared_ptr<Skree::Client>> ActiveClients;
        };
    }
}

#include "../server.hpp" // sorry
