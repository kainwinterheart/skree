#include "client.cpp"

namespace Skree {
    namespace Workers {
        void Client::client_thread() {
            struct ev_loop* loop = ev_loop_new(0);

            while(true) {
                ev_run(loop, EVRUN_NOWAIT);

                if(!new_clients.empty()) {
                    pthread_mutex_lock(&new_clients_mutex);

                    if(!new_clients.empty()) {
                        new_client_t* new_client = new_clients.front();
                        new_clients.pop();

                        pthread_mutex_unlock(&new_clients_mutex);

                        Client* client = new Client(
                            new_client->fh,
                            loop,
                            new_client->s_in,
                            new_client->s_in_len
                        );

                        if(new_client->cb != NULL)
                            new_client->cb(client);

                        free(new_client);

                    } else {
                        pthread_mutex_unlock(&new_clients_mutex);
                    }
                }
            }
        }
    }
}
