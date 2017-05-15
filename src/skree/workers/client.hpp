#pragma once

#include "../base/worker.hpp"
#include "../utils/misc.hpp"

#include <queue>
#include <deque>

namespace Skree {
    struct new_client_t;

    namespace Workers {
        struct ClientArgs {
            std::queue<std::shared_ptr<new_client_t>> queue;
            Utils::TSpinLock mutex;
            int fds[2];

            ClientArgs() {
                if(socketpair(PF_LOCAL, SOCK_STREAM, 0, fds) == -1) {
                    perror("socketpair");
                    abort();
                }
            }

            ~ClientArgs() {
                for(int i = 0; i < 2; ++i) {
                    close(fds[i]);
                }
            }
        };

        class Client : public Skree::Base::Worker {
        public:
            Client(Skree::Server& _server, void* _args = nullptr)
                : Skree::Base::Worker(_server, _args)
            {
                AcceptContext = WakeupContext = '\0';

                if(socketpair(PF_LOCAL, SOCK_STREAM, 0, WakeupFds) == -1) {
                    perror("socketpair");
                    abort();
                }
            }

            virtual void run() override;
            void accept();

            virtual ~Client() {
            }

        private:
            std::deque<std::shared_ptr<Skree::Client>> ActiveClients;
            int WakeupFds[2];
            char WakeupContext;
            char AcceptContext;
        };
    }
}

// #include "../server.hpp" // sorry
