#include "client.hpp"
#include "../utils/muhev.hpp"

namespace Skree {
    namespace Workers {
        void Client::run() {
            NMuhEv::TLoop loop;

            while(true) {
                auto list = NMuhEv::MakeEvList((ActiveClients.size() * 2) + 1);
                decltype(ActiveClients) newActiveClients;

                for(auto client : ActiveClients) {
                    if(!client->is_alive()) {
                        continue;
                    }

                    newActiveClients.push_back(client);

                    NMuhEv::TEvSpec spec {
                        .Ident = (uintptr_t)client->GetFH(),
                        .Filter = NMuhEv::MUHEV_FILTER_READ,
                        .Flags = NMuhEv::MUHEV_FLAG_NONE,
                        .Ctx = client.get()
                    };

                    loop.AddEvent(spec, list);

                    if(client->ShouldWrite()) {
                        spec.Filter = NMuhEv::MUHEV_FILTER_WRITE;
                        spec.Flags = NMuhEv::MUHEV_FLAG_ONESHOT;

                        loop.AddEvent(spec, list);
                    }
                }

                ActiveClients.swap(newActiveClients);

                loop.AddEvent(NMuhEv::TEvSpec {
                    .Ident = (uintptr_t)((Args*)args)->fds[0],
                    .Filter = NMuhEv::MUHEV_FILTER_READ,
                    .Flags = NMuhEv::MUHEV_FLAG_NONE,
                    .Ctx = nullptr
                }, list);

                int triggeredCount = loop.Wait(list);
                // Utils::cluck(2, "%d", triggeredCount);
                if(triggeredCount < 0) {
                    perror("kevent");
                    abort();

                } else if(triggeredCount > 0) {
                    for(int i = 0; i < triggeredCount; ++i) {
                        const auto& event = NMuhEv::GetEvent(list, i);

                        if(event.Ctx == nullptr) {
                            // if(event.Ident == ((Args*)args)->fds[0]) {
                                // Utils::cluck(2, "pipe: %d", event.Data);
                                if(event.Flags & EV_EOF) {
                                    Utils::cluck(1, "EV_EOF");
                                }

                                if(event.Data > 0) {
                                    char* dummy = (char*)malloc(event.Data);
                                    int j = 0;

                                    while(j < event.Data) {
                                        // Utils::cluck(3, "%d/%d", j, event.Data);
                                        int rv = ::read(event.Ident, dummy, event.Data - j);

                                        if(rv > 0) {
                                            j += rv;

                                        } else {
                                            break;
                                        }
                                    }

                                    free(dummy);
                                }

                                // Utils::cluck(2, "accept(): %lld", event.data);
                                accept();

                            // } else {
                            //     Utils::cluck(1, "wut");
                            // }

                        } else {
                            auto client = (Skree::Client*)event.Ctx;

                            if(client->is_alive()) {
                                // Utils::cluck(1, "client_cb()");
                                client->client_cb(event);

                            } else {
                                // TODO
                                // Utils::cluck(1, "dead client");
                            }
                        }
                    }
                }
            }
        }

        void Client::accept() {
            pthread_mutex_lock(((Args*)args)->mutex.get());

            while(!((Args*)args)->queue->empty()) {
                auto new_client = ((Args*)args)->queue->front();
                ((Args*)args)->queue->pop();

                auto client = std::make_shared<Skree::Client>(
                    new_client->fh,
                    // ((Args*)args)->loop,
                    new_client->s_in,
                    server
                );

                new_client->cb(*client);
                ActiveClients.push_back(client);

                // delete new_client; // TODO: seems to be unnecessary since it's std::shared_ptr
            }

            pthread_mutex_unlock(((Args*)args)->mutex.get());
        }
    }
}
