#include "discovery.hpp"

namespace Skree {
    namespace Workers {
        void Discovery::run() {
            while(true) {
                auto& peers_to_discover = server.peers_to_discover;
                peers_to_discover.lock();
                auto peers_to_discover_copy = peers_to_discover;
                peers_to_discover.unlock();

                for(auto& it : peers_to_discover_copy) {
                    auto peer_to_discover = it.second;

                    sockaddr_in* addr;
                    socklen_t addr_len;
                    int fh;

                    if(!do_connect(peer_to_discover->host, peer_to_discover->port, addr, addr_len, fh)) {
                        continue;
                    }

                    char* conn_name = Utils::get_host_from_sockaddr_in(addr);
                    uint32_t conn_port = Utils::get_port_from_sockaddr_in(addr);
                    char* conn_id = Utils::make_peer_id(strlen(conn_name), conn_name, conn_port);

                    free(conn_name);
                    bool found = false;

                    {
                        auto& known_peers_by_conn_id = server.known_peers_by_conn_id;
                        auto known_peers_by_conn_id_end = known_peers_by_conn_id.lock();
                        auto it = known_peers_by_conn_id.find(conn_id);
                        known_peers_by_conn_id.unlock();

                        found = (it != known_peers_by_conn_id_end);
                    }

                    if(!found) {
                        auto& known_peers = server.known_peers;
                        auto known_peers_end = known_peers.lock();
                        auto it = known_peers.find(conn_id);
                        known_peers.unlock();

                        found = (it != known_peers_end);
                    }

                    if(!found) {
                        auto& me = server.me;
                        auto me_end = me.lock();
                        auto it = me.find(conn_id);
                        me.unlock();

                        found = (it != me_end);
                    }

                    free(conn_id);

                    if(found) {
                        shutdown(fh, SHUT_RDWR);
                        close(fh);
                        free(addr);
                        continue;
                    }

                    new_client_t* new_client = new new_client_t {
                        .fh = fh,
                        .cb = [this](Skree::Client& client) {
                            cb1(client);
                        },
                        .s_in = addr,
                        .s_in_len = addr_len
                    };

                    pthread_mutex_lock(&(server.new_clients_mutex));
                    server.new_clients.push(new_client);
                    // server.push_new_clients(new_client); // TODO
                    pthread_mutex_unlock(&(server.new_clients_mutex));
                }

                sleep(5);
            }
        }

        bool Discovery::do_connect(
            const char* host,
            uint32_t peer_port,
            sockaddr_in*& addr,
            socklen_t& addr_len,
            int& fh
        ) {
            addrinfo hints;
            addrinfo* service_info;

            memset(&hints, 0, sizeof(hints));
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_flags = AI_NUMERICSERV;

            char port[6];
            sprintf(port, "%d", peer_port);
            int rv;

            if((rv = getaddrinfo(host, port, &hints, &service_info)) != 0) {
                fprintf(stderr, "getaddrinfo(%s, %u): %s\n", host, peer_port, gai_strerror(rv));
                return false;
            }

            int yes = 1;
            addr = (sockaddr_in*)malloc(sizeof(*addr));
            bool connected = false;

            for(addrinfo* ai_it = service_info; ai_it != nullptr; ai_it = ai_it->ai_next) {
                if((fh = socket(ai_it->ai_family, ai_it->ai_socktype, ai_it->ai_protocol)) == -1) {
                    perror("socket");
                    continue;
                }

                if(setsockopt(fh, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)) == -1) {
                    perror("setsockopt");
                    close(fh);
                    continue;
                }

                if(setsockopt(fh, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) == -1) {
                    perror("setsockopt");
                    close(fh);
                    continue;
                }

                // TODO: discovery should be async too
                timeval tv;
                tv.tv_sec = (server.discovery_timeout_milliseconds / 1000);
                tv.tv_usec = ((server.discovery_timeout_milliseconds % 1000) * 1000);

                if(setsockopt(fh, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == -1) {
                    perror("setsockopt");
                    close(fh);
                    continue;
                }

                if(setsockopt(fh, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == -1) {
                    perror("setsockopt");
                    close(fh);
                    continue;
                }

                if(connect(fh, ai_it->ai_addr, ai_it->ai_addrlen) == -1) {
                    if(errno != EINPROGRESS) {
                        perror("connect");
                        close(fh);
                        continue;
                    }

                    fcntl(fh, F_SETFL, fcntl(fh, F_GETFL, 0) | O_NONBLOCK);
                }

                *addr = *(sockaddr_in*)(ai_it->ai_addr);
                addr_len = ai_it->ai_addrlen;
                connected = true;

                break;
            }

            freeaddrinfo(service_info);

            if(connected && (getpeername(fh, (sockaddr*)addr, &addr_len) == -1)) {
                perror("getpeername");
                close(fh);
                free(addr);
                connected = false;
            }

            return connected;
        }

        void Discovery::cb1(Skree::Client& client) {
            auto _cb = [this](
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item,
                Skree::Base::PendingRead::Callback::Args& args
            ) {
                return cb2(client, item, args);
            };

            const auto cb = new Skree::PendingReads::Callbacks::Discovery<decltype(_cb)>(server, _cb);
            const auto item = new Skree::Base::PendingRead::QueueItem {
                .len = 1,
                .cb = cb,
                .ctx = nullptr,
                .opcode = true,
                .noop = false
            };

            auto w_req = Skree::Actions::W::out_init();

            auto witem = new Skree::Base::PendingWrite::QueueItem {
                .len = w_req->len,
                .data = w_req->data,
                .pos = 0,
                .cb = item
            };

            client.push_write_queue(witem);
        }

        Skree::Base::PendingWrite::QueueItem* Discovery::cb6(
            Skree::Client& client,
            const Skree::Base::PendingRead::QueueItem& item,
            Skree::Base::PendingRead::Callback::Args& args
        ) {
            // for(int i = 0; i < item.len; ++i)
            //     printf("[discovery::cb6] read from %s [%d]: 0x%.2X\n", client.get_peer_id(),i,args.data[i]);

            if(args.data[0] == SKREE_META_OPCODE_K) {
                uint64_t in_pos = 1;
                uint32_t _tmp;

                memcpy(&_tmp, args.data + in_pos, sizeof(_tmp));
                in_pos += sizeof(_tmp);
                uint32_t cnt = ntohl(_tmp);

                uint32_t host_len;
                char* host;
                uint32_t port;
                char* _peer_id;
                bool got_new_peers = false;
                peers_to_discover_t::iterator prev_item;
                peers_to_discover_t::iterator peers_to_discover_end;
                auto& peers_to_discover = server.peers_to_discover;

                while(cnt > 0) {
                    --cnt;
                    memcpy(&_tmp, args.data + in_pos, sizeof(_tmp));
                    in_pos += sizeof(_tmp);
                    host_len = ntohl(_tmp);

                    host = (char*)malloc(host_len + 1);
                    memcpy(host, args.data + in_pos, host_len);
                    in_pos += host_len;
                    host[host_len] = '\0';

                    memcpy(&_tmp, args.data + in_pos, sizeof(_tmp));
                    in_pos += sizeof(_tmp);
                    port = ntohl(_tmp);

                    _peer_id = Utils::make_peer_id(host_len, host, port);

                    peers_to_discover_end = peers_to_discover.lock();
                    prev_item = peers_to_discover.find(_peer_id);

                    if(prev_item == peers_to_discover_end) {
                        // printf("[discovery] fill peers_to_discover: %s:%u\n", host, port);
                        peers_to_discover[_peer_id] = new peer_to_discover_t {
                            .host = host,
                            .port = port
                        };

                        got_new_peers = true;

                    } else {
                        free(_peer_id);
                        free(host);
                    }

                    peers_to_discover.unlock();
                }

                if(got_new_peers)
                    server.save_peers_to_discover();
            }

            return (Skree::Base::PendingWrite::QueueItem*)nullptr;
        }

        Skree::Base::PendingWrite::QueueItem* Discovery::cb5(
            Skree::Client& client,
            const Skree::Base::PendingRead::QueueItem& item,
            Skree::Base::PendingRead::Callback::Args& args
        ) {
            // printf("DISCOVERY CB5 OPCODE: %c\n", args.data[0]);
            if(args.data[0] == SKREE_META_OPCODE_K) {
                auto& known_peers = server.known_peers;
                auto& known_peers_by_conn_id = server.known_peers_by_conn_id;
                auto known_peers_end = known_peers.lock();
                known_peers_by_conn_id.lock();

                // peer_id is guaranteed to be set here
                const auto& peer_id = client.get_peer_id();
                const auto& conn_id = client.get_conn_id();
                auto known_peer = known_peers.find(peer_id);

                if(known_peer != known_peers_end) {
                    auto& list = known_peer->second;

                    for(const auto& peer : list) {
                        if(strcmp(conn_id, peer->get_conn_id()) == 0) {
                            args.stop = true;
                            break;
                        }
                    }
                }

                if(!args.stop) {
                    known_peers[peer_id].push_back(&client);
                    known_peers_by_conn_id[conn_id].push_back(&client);
                }

                known_peers_by_conn_id.unlock();
                known_peers.unlock();

                if(!args.stop) {
                    // TODO
                    for(int i = 0; i < 10; ++i) {
                        const auto& peer_name = client.get_peer_name();
                        const auto& peer_port = client.get_peer_port();
                        sockaddr_in* addr;
                        socklen_t addr_len;
                        int fh;

                        if(!do_connect(peer_name, peer_port, addr, addr_len, fh)) {
                            break;
                        }

                        const auto& peer_id_clone = strdup(peer_id);
                        const auto& peer_name_clone = strdup(peer_name);
                        const auto& peer_name_len = client.get_peer_name_len();

                        new_client_t* new_client = new new_client_t {
                            .fh = fh,
                            .cb = [
                                this,
                                peer_name_clone,
                                peer_port,
                                peer_id_clone,
                                peer_name_len
                            ](Skree::Client& client) {
                                client.set_peer_name(peer_name_len, peer_name_clone);
                                client.set_peer_port(peer_port);
                                client.set_peer_id(peer_id_clone);

                                auto _cb = [this, peer_id_clone](
                                    Skree::Client& client,
                                    const Skree::Base::PendingRead::QueueItem& item,
                                    Skree::Base::PendingRead::Callback::Args& args
                                ) {
                                    if(args.data[0] != SKREE_META_OPCODE_K)
                                        return nullptr;

                                    auto& known_peers = server.known_peers;
                                    auto& known_peers_by_conn_id = server.known_peers_by_conn_id;

                                    auto known_peers_end = known_peers.lock();
                                    known_peers_by_conn_id.lock();

                                    const auto& conn_id = client.get_conn_id();

                                    known_peers[peer_id_clone].push_back(&client);
                                    known_peers_by_conn_id[conn_id].push_back(&client);

                                    known_peers_by_conn_id.unlock();
                                    known_peers.unlock();

                                    return nullptr;
                                };

                                const auto cb = new Skree::PendingReads::Callbacks::Discovery<decltype(_cb)>(server, _cb);
                                const auto item = new Skree::Base::PendingRead::QueueItem {
                                    .len = 1,
                                    .cb = cb,
                                    .ctx = nullptr,
                                    .opcode = true,
                                    .noop = false
                                };

                                auto h_req = Skree::Actions::H::out_init(server);

                                auto witem = new Skree::Base::PendingWrite::QueueItem {
                                    .len = h_req->len,
                                    .data = h_req->data,
                                    .pos = 0,
                                    .cb = item
                                };

                                client.push_write_queue(witem);
                            },
                            .s_in = addr,
                            .s_in_len = addr_len
                        };

                        pthread_mutex_lock(&(server.new_clients_mutex));
                        server.new_clients.push(new_client);
                        // server.push_new_clients(new_client); // TODO
                        pthread_mutex_unlock(&(server.new_clients_mutex));
                    }

                    auto _cb = [this](
                        Skree::Client& client,
                        const Skree::Base::PendingRead::QueueItem& item,
                        Skree::Base::PendingRead::Callback::Args& args
                    ) {
                        return cb6(client, item, args);
                    };

                    const auto cb = new Skree::PendingReads::Callbacks::Discovery<decltype(_cb)>(server, _cb);
                    const auto item = new Skree::Base::PendingRead::QueueItem {
                        .len = 1,
                        .cb = cb,
                        .ctx = nullptr,
                        .opcode = true,
                        .noop = false
                    };

                    auto l_req = Skree::Actions::L::out_init();

                    return new Skree::Base::PendingWrite::QueueItem {
                        .len = l_req->len,
                        .data = l_req->data,
                        .pos = 0,
                        .cb = item
                    };
                }

            } else {
                args.stop = true;
            }

            return (Skree::Base::PendingWrite::QueueItem*)nullptr;
        }

        Skree::Base::PendingWrite::QueueItem* Discovery::cb2(
            Skree::Client& client,
            const Skree::Base::PendingRead::QueueItem& item,
            Skree::Base::PendingRead::Callback::Args& args
        ) {
            // printf("DISCOVERY CB2 OPCODE: %c\n", args.data[0]);
            if(args.data[0] == SKREE_META_OPCODE_K) {
                uint64_t in_pos = 1;
                uint32_t _tmp;

                memcpy(&_tmp, args.data + in_pos, sizeof(_tmp));
                in_pos += sizeof(_tmp);
                uint32_t len = ntohl(_tmp);

                char* peer_name = (char*)malloc(len + 1);
                memcpy(peer_name, args.data + in_pos, len);
                in_pos += len;
                peer_name[len] = '\0';

                _tmp = client.get_conn_port();
                char* _peer_id = Utils::make_peer_id(len, peer_name, _tmp);
                bool accepted = false;

                auto& known_peers = server.known_peers;
                auto known_peers_end = known_peers.lock();
                auto known_peer = known_peers.find(_peer_id);
                known_peers.unlock();

                if(known_peer == known_peers_end) {
                    if(strcmp(_peer_id, server.my_peer_id) == 0) {
                        auto& me = server.me;
                        auto me_end = me.lock();
                        auto it = me.find(_peer_id);

                        if(it == me_end) {
                            char* _conn_peer_id = client.get_conn_id();

                            server.me[_peer_id] = true;
                            server.me[strdup(_conn_peer_id)] = true;

                        } else {
                            free(_peer_id);
                        }

                        me.unlock();

                        free(peer_name);

                    } else {
                        client.set_peer_name(len, peer_name);
                        client.set_peer_port(_tmp);
                        client.set_peer_id(_peer_id);

                        accepted = true;
                    }

                } else {
                    free(_peer_id);
                    free(peer_name);
                }

                if(accepted) {
                    auto _cb = [this](
                        Skree::Client& client,
                        const Skree::Base::PendingRead::QueueItem& item,
                        Skree::Base::PendingRead::Callback::Args& args
                    ) {
                        return cb5(client, item, args);
                    };

                    const auto cb = new Skree::PendingReads::Callbacks::Discovery<decltype(_cb)>(server, _cb);
                    const auto item = new Skree::Base::PendingRead::QueueItem {
                        .len = 1,
                        .cb = cb,
                        .ctx = nullptr,
                        .opcode = true,
                        .noop = false
                    };

                    auto h_req = Skree::Actions::H::out_init(server);

                    auto witem = new Skree::Base::PendingWrite::QueueItem {
                        .len = h_req->len,
                        .data = h_req->data,
                        .pos = 0,
                        .cb = item
                    };

                    return witem;

                } else {
                    args.stop = true;
                }

            } else {
                args.stop = true;
            }

            return (Skree::Base::PendingWrite::QueueItem*)nullptr;
        }
    }
}
