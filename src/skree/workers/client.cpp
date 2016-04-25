#include "client.hpp"

namespace Skree {
    namespace Workers {
        void Client::run() {
            struct ev_loop* loop = ev_loop_new(0);

            while(true) {
                ev_run(loop, EVRUN_NOWAIT);

                if(!server.new_clients.empty()) {
                    pthread_mutex_lock(&(server.new_clients_mutex));

                    if(!server.new_clients.empty()) {
                        new_client_t* new_client = server.new_clients.front();
                        server.new_clients.pop();

                        pthread_mutex_unlock(&(server.new_clients_mutex));

                        Skree::Client* client = new Skree::Client(
                            new_client->fh,
                            loop,
                            new_client->s_in,
                            new_client->s_in_len,
                            server
                        );

                        new_client->cb(*client);

                        free(new_client);

                    } else {
                        pthread_mutex_unlock(&(server.new_clients_mutex));
                    }
                }
            }
        }
    }
}
