#include "client.hpp"

namespace Skree {
    namespace Workers {
        void Client::run() {
            ev_run(((Args*)args)->loop, 0);
        }

        void Client::async_cb(struct ev_loop* loop, ev_async* _watcher, int events) {
            ((Client::bound_ev_async*)_watcher)->worker->accept();
        }

        void Client::accept() {
            pthread_mutex_lock(((Args*)args)->mutex.get());

            while(!((Args*)args)->queue->empty()) {
                auto new_client = ((Args*)args)->queue->front();
                ((Args*)args)->queue->pop();

                Skree::Client* client = new Skree::Client(
                    new_client->fh,
                    ((Args*)args)->loop,
                    new_client->s_in,
                    server
                );

                new_client->cb(*client);

                // delete new_client; // TODO: seems to be unnecessary since it's std::shared_ptr
            }

            pthread_mutex_unlock(((Args*)args)->mutex.get());
        }
    }
}
