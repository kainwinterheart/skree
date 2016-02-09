#include "discovery.hpp"

namespace Skree {
    namespace Workers {
        void Discovery::run() { // TODO
            while(true) {
                for(
                    peers_to_discover_t::const_iterator it = peers_to_discover.cbegin();
                    it != peers_to_discover.cend();
                    ++it
                ) {
                    peer_to_discover_t* peer_to_discover = it->second;

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

                        timeval tv;
                        tv.tv_sec = (discovery_timeout_milliseconds / 1000);
                        tv.tv_usec = ((discovery_timeout_milliseconds % 1000) * 1000);

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
                            char* conn_name = get_host_from_sockaddr_in(addr);
                            uint32_t conn_port = get_port_from_sockaddr_in(addr);
                            char* conn_id = make_peer_id(strlen(conn_name), conn_name, conn_port);

                            free(conn_name);
                            bool found = false;

                            {
                                pthread_mutex_lock(&known_peers_mutex);

                                known_peers_t::const_iterator it =
                                    known_peers_by_conn_id.find(conn_id);

                                if(it != known_peers_by_conn_id.cend())
                                    found = true;

                                pthread_mutex_unlock(&known_peers_mutex);
                            }

                            if(!found) {
                                pthread_mutex_lock(&known_peers_mutex);

                                known_peers_t::const_iterator it = known_peers.find(conn_id);

                                if(it != known_peers.cend())
                                    found = true;

                                pthread_mutex_unlock(&known_peers_mutex);
                            }

                            if(!found) {
                                pthread_mutex_lock(&me_mutex);

                                me_t::const_iterator it = me.find(conn_id);

                                if(it != me.cend())
                                    found = true;

                                pthread_mutex_unlock(&me_mutex);
                            }

                            free(conn_id);

                            if(found) {
                                shutdown(fh, SHUT_RDWR);
                                close(fh);
                                free(addr);

                            } else {
                                new_client_t* new_client = (new_client_t*)malloc(
                                    sizeof(*new_client));

                                new_client->fh = fh;
                                new_client->cb = discovery_cb1;
                                new_client->s_in = addr;
                                new_client->s_in_len = addr_len;

                                pthread_mutex_lock(&new_clients_mutex);
                                new_clients.push(new_client);
                                pthread_mutex_unlock(&new_clients_mutex);
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
    }
}
