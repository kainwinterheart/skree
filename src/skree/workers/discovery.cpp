#include "discovery.hpp"

namespace Skree {
    namespace Workers {
        void Discovery::run() {
            while(true) {
                for(
                    auto it = server.peers_to_discover.cbegin();
                    it != server.peers_to_discover.cend();
                    ++it
                ) {
                    auto peer_to_discover = it->second;

                    addrinfo hints;
                    addrinfo* service_info;

                    memset(&hints, 0, sizeof(hints));
                    hints.ai_family = AF_UNSPEC;
                    hints.ai_socktype = SOCK_STREAM;
                    hints.ai_flags = AI_NUMERICSERV;

                    char port[6];
                    sprintf(port, "%d", peer_to_discover->port);
                    int rv;

                    if((rv = getaddrinfo(peer_to_discover->host, port, &hints, &service_info)) != 0) {
                        fprintf(stderr, "getaddrinfo(%s, %u): %s\n",
                            peer_to_discover->host, peer_to_discover->port, gai_strerror(rv));
                        continue;
                    }

                    int fh;
                    int yes = 1;
                    sockaddr_in* addr = (sockaddr_in*)malloc(sizeof(*addr));
                    socklen_t addr_len;
                    bool connected = false;

                    for(addrinfo* ai_it = service_info; ai_it != NULL; ai_it = ai_it->ai_next) {
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

                    if(connected) {
                        if(getpeername(fh, (sockaddr*)addr, &addr_len) == -1) {
                            perror("getpeername");
                            close(fh);
                            free(addr);

                        } else {
                            char* conn_name = Utils::get_host_from_sockaddr_in(addr);
                            uint32_t conn_port = Utils::get_port_from_sockaddr_in(addr);
                            char* conn_id = Utils::make_peer_id(strlen(conn_name), conn_name, conn_port);

                            free(conn_name);
                            bool found = false;

                            {
                                pthread_mutex_lock(&(server.known_peers_mutex));

                                known_peers_t::const_iterator it =
                                    server.known_peers_by_conn_id.find(conn_id);

                                if(it != server.known_peers_by_conn_id.cend())
                                    found = true;

                                pthread_mutex_unlock(&(server.known_peers_mutex));
                            }

                            if(!found) {
                                pthread_mutex_lock(&(server.known_peers_mutex));

                                known_peers_t::const_iterator it =
                                    server.known_peers.find(conn_id);

                                if(it != server.known_peers.cend())
                                    found = true;

                                pthread_mutex_unlock(&(server.known_peers_mutex));
                            }

                            if(!found) {
                                pthread_mutex_lock(&(server.me_mutex));

                                me_t::const_iterator it = server.me.find(conn_id);

                                if(it != server.me.cend())
                                    found = true;

                                pthread_mutex_unlock(&(server.me_mutex));
                            }

                            free(conn_id);

                            if(found) {
                                shutdown(fh, SHUT_RDWR);
                                close(fh);
                                free(addr);

                            } else {
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
                        }

                    } else {
                        fprintf(stderr, "Peer %s is unreachable\n", it->first);
                        free(addr);
                    }
                }

                sleep(5);
            }
        }

        void Discovery::cb1(Skree::Client& client) {
            auto _cb = [this](
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item,
                const Skree::Base::PendingRead::Callback::Args& args
            ) {
                return cb2(client, item, args);
            };

            const auto cb = new Skree::PendingReads::Callbacks::Discovery<decltype(_cb)>(server, _cb);
            const auto item = new Skree::Base::PendingRead::QueueItem {
                .len = 1,
                .cb = cb,
                .ctx = NULL,
                .opcode = true
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

        const Skree::Base::PendingRead::QueueItem* Discovery::cb6(
            Skree::Client& client,
            const Skree::Base::PendingRead::QueueItem& item,
            const Skree::Base::PendingRead::Callback::Args& args
        ) {
            if(args.data[0] == SKREE_META_OPCODE_K) {
                uint64_t in_pos = 0;
                uint32_t _tmp;

                memcpy(&_tmp, args.data + in_pos, sizeof(_tmp));
                in_pos += sizeof(_tmp);
                uint32_t cnt = ntohl(_tmp);
                uint32_t host_len;
                char* host;
                uint32_t port;
                char* _peer_id;
                peer_to_discover_t* peer_to_discover;
                bool got_new_peers = false;
                peers_to_discover_t::const_iterator prev_item;

                pthread_mutex_lock(&(server.peers_to_discover_mutex));

                while(cnt > 0) {
                    --cnt;
                    memcpy(&_tmp, args.data + in_pos, sizeof(_tmp));
                    host_len = ntohl(_tmp);

                    host = (char*)malloc(host_len + 1);
                    memcpy(host, args.data + in_pos, host_len);
                    in_pos += host_len;
                    host[host_len] = '\0';

                    memcpy(&_tmp, args.data + in_pos, sizeof(_tmp));
                    in_pos += sizeof(_tmp);
                    port = ntohl(_tmp);

                    _peer_id = Utils::make_peer_id(host_len, host, port);

                    prev_item = server.peers_to_discover.find(_peer_id);

                    if(prev_item == server.peers_to_discover.cend()) {
                        peer_to_discover = (peer_to_discover_t*)malloc(
                            sizeof(*peer_to_discover));

                        peer_to_discover->host = host;
                        peer_to_discover->port = port;

                        server.peers_to_discover[_peer_id] = peer_to_discover;
                        got_new_peers = true;

                    } else {
                        free(_peer_id);
                        free(host);
                    }
                }

                if(got_new_peers)
                    server.save_peers_to_discover();

                pthread_mutex_unlock(&(server.peers_to_discover_mutex));
            }

            return Skree::PendingReads::noop(server);
        }

        const Skree::Base::PendingRead::QueueItem* Discovery::cb5(
            Skree::Client& client,
            const Skree::Base::PendingRead::QueueItem& item,
            const Skree::Base::PendingRead::Callback::Args& args
        ) {
            if(args.data[0] == SKREE_META_OPCODE_K) {
                pthread_mutex_lock(&(server.known_peers_mutex));

                // peer_id is guaranteed to be set here
                known_peers_t::const_iterator known_peer =
                    server.known_peers.find(client.get_peer_id());

                if(known_peer == server.known_peers.cend()) {
                    server.known_peers[client.get_peer_id()] = &client;
                    server.known_peers_by_conn_id[client.get_conn_id()] = &client;

                } else {
                    args.stop = true;
                }

                pthread_mutex_unlock(&(server.known_peers_mutex));

                if(!args.stop) {
                    auto _cb = [this](
                        Skree::Client& client,
                        const Skree::Base::PendingRead::QueueItem& item,
                        const Skree::Base::PendingRead::Callback::Args& args
                    ) {
                        return cb6(client, item, args);
                    };

                    const auto cb = new Skree::PendingReads::Callbacks::Discovery<decltype(_cb)>(server, _cb);
                    const auto item = new Skree::Base::PendingRead::QueueItem {
                        .len = 1,
                        .cb = cb,
                        .ctx = NULL,
                        .opcode = true
                    };

                    auto l_req = Skree::Actions::L::out_init();

                    auto witem = new Skree::Base::PendingWrite::QueueItem {
                        .len = l_req->len,
                        .data = l_req->data,
                        .pos = 0,
                        .cb = item
                    };

                    client.push_write_queue(witem);
                }

            } else {
                args.stop = true;
            }

            return Skree::PendingReads::noop(server);
        }

        const Skree::Base::PendingRead::QueueItem* Discovery::cb2(
            Skree::Client& client,
            const Skree::Base::PendingRead::QueueItem& item,
            const Skree::Base::PendingRead::Callback::Args& args
        ) {
            if(args.data[0] == SKREE_META_OPCODE_K) {
                uint64_t in_pos = 0;
                uint32_t _tmp;

                memcpy(&_tmp, args.data + in_pos, sizeof(_tmp));
                in_pos += sizeof(_tmp);
                uint32_t len = ntohl(_tmp);

                char* peer_name = (char*)malloc(len);
                memcpy(peer_name, args.data + in_pos, len);
                in_pos += len;

                _tmp = client.get_conn_port();
                char* _peer_id = Utils::make_peer_id(len, peer_name, _tmp);
                bool accepted = false;

                pthread_mutex_lock(&(server.known_peers_mutex));

                known_peers_t::const_iterator known_peer =
                    server.known_peers.find(_peer_id);

                if(known_peer == server.known_peers.cend()) {
                    if(strcmp(_peer_id, server.my_peer_id) == 0) {
                        pthread_mutex_lock(&(server.me_mutex));

                        me_t::const_iterator it = server.me.find(_peer_id);

                        if(it == server.me.cend()) {
                            char* _conn_peer_id = client.get_conn_id();

                            server.me[_peer_id] = true;
                            server.me[strdup(_conn_peer_id)] = true;

                        } else {
                            free(_peer_id);
                        }

                        pthread_mutex_unlock(&(server.me_mutex));

                    } else {
                        client.set_peer_name(len, peer_name);
                        client.set_peer_port(_tmp);
                        client.set_peer_id(_peer_id);

                        accepted = true;
                    }

                } else {
                    free(_peer_id);
                }

                pthread_mutex_unlock(&(server.known_peers_mutex));

                if(accepted) {
                    auto _cb = [this](
                        Skree::Client& client,
                        const Skree::Base::PendingRead::QueueItem& item,
                        const Skree::Base::PendingRead::Callback::Args& args
                    ) {
                        return cb5(client, item, args);
                    };

                    const auto cb = new Skree::PendingReads::Callbacks::Discovery<decltype(_cb)>(server, _cb);
                    const auto item = new Skree::Base::PendingRead::QueueItem {
                        .len = 1,
                        .cb = cb,
                        .ctx = NULL,
                        .opcode = true
                    };

                    auto h_req = Skree::Actions::H::out_init(server);

                    auto witem = new Skree::Base::PendingWrite::QueueItem {
                        .len = h_req->len,
                        .data = h_req->data,
                        .pos = 0,
                        .cb = item
                    };

                    client.push_write_queue(witem);

                } else {
                    args.stop = true;
                }

            } else {
                args.stop = true;
            }

            return Skree::PendingReads::noop(server);
        }
    }
}
