#include "client.hpp"
#include "../utils/muhev.hpp"
#include "../client.hpp"

namespace Skree {
    namespace Workers {
        void Client::run() {
            NMuhEv::TLoop loop;

            while(true) {
                auto list = NMuhEv::MakeEvList(ActiveClients.size() + 2);
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
                    .Ident = (uintptr_t)((ClientArgs*)args)->fds[0],
                    .Filter = NMuhEv::MUHEV_FILTER_READ,
                    .Flags = NMuhEv::MUHEV_FLAG_NONE,
                    .Ctx = &AcceptContext
                }, list);

                loop.AddEvent(NMuhEv::TEvSpec {
                    .Ident = (uintptr_t)WakeupFds[0],
                    .Filter = NMuhEv::MUHEV_FILTER_READ,
                    .Flags = NMuhEv::MUHEV_FLAG_NONE,
                    .Ctx = &WakeupContext
                }, list);

                int triggeredCount = loop.Wait(list);
                // Utils::cluck(3, "Got %d events in thread %llu", triggeredCount, Utils::ThreadId());
                if(triggeredCount < 0) {
                    perror("kevent");
                    abort();

                } else if(triggeredCount > 0) {
                    for(int i = 0; i < triggeredCount; ++i) {
                        const auto& event = NMuhEv::GetEvent(list, i);

                        if((event.Ctx == &AcceptContext) || (event.Ctx == &WakeupContext)) {
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
                                if(event.Ctx == &AcceptContext) {
                                    accept();
                                }

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
            Utils::TSpinLockGuard guard(((ClientArgs*)args)->mutex);

            while(!((ClientArgs*)args)->queue.empty()) {
                auto new_client = ((ClientArgs*)args)->queue.front();
                ((ClientArgs*)args)->queue.pop();

                auto client = std::make_shared<Skree::Client>(
                    new_client->fh,
                    WakeupFds[1],
                    new_client->s_in,
                    server
                );

                client->SetWeakPtr(client);
                new_client->cb(*client);
                ActiveClients.push_back(client);

                // delete new_client; // TODO: seems to be unnecessary since it's std::shared_ptr
            }
        }
    }
}
