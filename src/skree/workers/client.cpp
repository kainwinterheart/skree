#include "client.hpp"
#include "../utils/muhev.hpp"

namespace Skree {
    namespace Workers {
        void Client::run() {
            NMuhEv::TLoop loop;

            while(true) {
                auto list = NMuhEv::MakeEvList(ActiveClients.size() + 1);
                decltype(ActiveClients) newActiveClients;

                for(auto client : ActiveClients) {
                    if(!client->is_alive()) {
                        continue;
                    }

                    newActiveClients.push_back(client);

                    loop.AddEvent(NMuhEv::TEvSpec {
                        .Ident = (uintptr_t)client->GetFH(),
                        .Filter = (NMuhEv::MUHEV_FILTER_READ | (
                            client->ShouldWrite() ? NMuhEv::MUHEV_FILTER_WRITE : 0
                        )),
                        .Flags = NMuhEv::MUHEV_FLAG_NONE,
                        .Ctx = client.get()
                    }, list);
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
                                if(event.Flags & NMuhEv::MUHEV_FLAG_EOF) {
                                    Utils::cluck(1, "EV_EOF");
                                }

                                if(event.Flags & NMuhEv::MUHEV_FLAG_ERROR) {
                                    Utils::cluck(1, "EV_ERROR");
                                }

                                {
                                    char dummy[128];
                                    int rv = recvfrom(
                                        event.Ident,
                                        dummy,
                                        128,
                                        MSG_DONTWAIT,
                                        NULL,
                                        0
                                    );

                                    if(rv < 0) {
                                        perror("recvfrom");
                                        abort();
                                    }
                                }

                                // Utils::cluck(1, "accept()");
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
                } else {
                    // Utils::cluck(1, "nothing is here");
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
