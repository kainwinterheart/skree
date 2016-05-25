#include "client.hpp"
// #include "base/pending_read.hpp"

// #define SKREE_NET_DEBUG

namespace Skree {
    void Client::ordinary_packet_cb(
        const char& opcode, char*& out_data,
        size_t& out_len, const size_t& in_packet_len
    ) {
        if(handlers[opcode] == NULL) {
            printf("Unknown packet header: 0x%.2X\n", opcode);

        } else {
            // TODO: should be one pending read instead of two
            auto _cb = [this, opcode](
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item,
                Skree::Base::PendingRead::Callback::Args& args
            ) {
                auto _cb = [this, opcode](
                    Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item,
                    Skree::Base::PendingRead::Callback::Args& args
                ) {
                    uint64_t _out_len = 0; // TODO: get rid of this
                    handlers[opcode]->in(item.len, args.data, _out_len, args.out_data);
                    args.out_len = _out_len;
                    ++(server.stat_num_requests);

                    return (Skree::Base::PendingWrite::QueueItem*)NULL;
                };

                uint32_t _tmp;
                memcpy(&_tmp, args.data, sizeof(_tmp));
                _tmp = ntohl(_tmp);

                if(_tmp == 0) {
                    _cb(client, item, args);

                } else {
                    const auto cb = new Skree::PendingReads::Callbacks::OrdinaryPacket<decltype(_cb)>(server, _cb);
                    const auto _item = new Skree::Base::PendingRead::QueueItem {
                        .len = _tmp,
                        .cb = cb,
                        .ctx = NULL,
                        .opcode = false,
                        .noop = false
                    };

                    push_pending_reads_queue(_item, true);
                }

                return (Skree::Base::PendingWrite::QueueItem*)NULL;
            };

            const auto cb = new Skree::PendingReads::Callbacks::OrdinaryPacket<decltype(_cb)>(server, _cb);
            const auto item = new Skree::Base::PendingRead::QueueItem {
                .len = 4,
                .cb = cb,
                .ctx = NULL,
                .opcode = false,
                .noop = false
            };

            push_pending_reads_queue(item, true);
        }
    }

    void Client::read_cb() {
        while(read_queue_length > 0) {
            size_t in_packet_len = 1;
            size_t out_len = 0;

            char opcode = read_queue[0];
            char* out_data = NULL;

            if(!pending_reads.empty()) {
                // Pending reads queue is a top priority callback
                // If there is a pending read - incoming data should
                // be passed to such a callback
#ifdef SKREE_NET_DEBUG
                printf("Got opcode: 0x%.2X, pending_reads not empty\n", opcode);
#endif
                auto item = pending_reads.front();

                if(
                    (item->opcode == true)
                    && (
                        (opcode == SKREE_META_OPCODE_K) || (opcode == SKREE_META_OPCODE_F)
                        || (opcode == SKREE_META_OPCODE_A)
                    )
                ) {
                    pending_reads.pop_front();

                    auto _cb = [this, opcode, item](
                        Skree::Client& client,
                        const Skree::Base::PendingRead::QueueItem& _item,
                        Skree::Base::PendingRead::Callback::Args& args
                    ) {
                        auto _cb = [this, opcode, item](
                            Skree::Client& client,
                            const Skree::Base::PendingRead::QueueItem& _item,
                            Skree::Base::PendingRead::Callback::Args& args
                        ) {
                            char data [_item.len + 1];
                            data[0] = opcode;

                            if(_item.len > 0) {
                                memcpy(data + 1, args.data, _item.len); // TODO
                            }

                            const char* _data = data;

                            // ***
                            Skree::Base::PendingRead::Callback::Args _args {
                                .data = _data,
                                .out_data = args.out_data,
                                .out_len = args.out_len,
                                .stop = args.stop
                            };

                            auto _cb = item->cb;
                            const Skree::Base::PendingRead::QueueItem __item {
                                .len = _item.len + 1,
                                .cb = _cb,
                                .ctx = item->ctx,
                                .opcode = true,
                                .noop = false
                            };
#ifdef SKREE_NET_DEBUG
                            printf("About to run original callback with a packet of size %lu bytes\n", __item.len);
#endif
                            auto new_item = _cb->run(*this, __item, _args);

                            if(new_item != NULL) {
                                push_write_queue(new_item);
                            }
                            // ***

                            return (Skree::Base::PendingWrite::QueueItem*)NULL;
                        };

                        uint32_t _tmp;
                        memcpy(&_tmp, args.data, sizeof(_tmp));
                        _tmp = ntohl(_tmp);

                        if(_tmp == 0) {
                            const Skree::Base::PendingRead::QueueItem __item {
                                .len = 0,
                                .cb = NULL,
                                .ctx = NULL,
                                .opcode = false,
                                .noop = false
                            };

                            _cb(client, __item, args);

                        } else {
                            const auto cb = new Skree::PendingReads::Callbacks::OrdinaryPacket<decltype(_cb)>(server, _cb);
                            const auto __item = new Skree::Base::PendingRead::QueueItem {
                                .len = _tmp,
                                .cb = cb,
                                .ctx = NULL,
                                .opcode = false,
                                .noop = false
                            };
#ifdef SKREE_NET_DEBUG
                            printf("Got opcode for pending read, need to read %lu more bytes\n", _tmp);
#endif
                            push_pending_reads_queue(__item, true);
                        }

                        return (Skree::Base::PendingWrite::QueueItem*)NULL;
                    };

                    const auto cb = new Skree::PendingReads::Callbacks::OrdinaryPacket<decltype(_cb)>(server, _cb);
                    const auto item = new Skree::Base::PendingRead::QueueItem {
                        .len = 4,
                        .cb = cb,
                        .ctx = NULL,
                        .opcode = false,
                        .noop = false
                    };

                    push_pending_reads_queue(item, true);

                } else if(item->opcode == false) {
                    // If pending read waits for opcode, and it is the
                    // reply opcode we've got here, or if pending read
                    // does not wait for opcode - process pending read

                    --in_packet_len;

                    if(read_queue_length >= item->len) {
#ifdef SKREE_NET_DEBUG
                        printf("Got raw pending read of size %lu bytes\n", item->len);
#endif
                        bool stop = false;
                        const char* _data = read_queue + in_packet_len;

                        pending_reads.pop_front();

                        // ***
                        Skree::Base::PendingRead::Callback::Args args {
                            .data = _data,
                            .out_data = out_data,
                            .out_len = out_len,
                            .stop = stop
                        };

                        auto _cb = item->cb;
                        auto new_item = _cb->run(*this, *item, args);
                        in_packet_len += (item->len - in_packet_len);

                        if(new_item != NULL) {
                            push_write_queue(new_item);
                        }
                        // ***

                        if(stop) {
                            delete this;
                            break;
                        }

                    } else {
                        // If we have a pending read, but requested amount
                        // of data is still not arrived - we should wait for it

                        break;
                    }

                } else {
                    // If pending read waits for opcode, and it is not
                    // the reply opcode we've got here - process data as
                    // ordinary inbound packet

                    // printf("ordinary_packet_cb 1\n");
#ifdef SKREE_NET_DEBUG
                    printf("Got opcode: 0x%.2X, pending_reads is empty, but queue item is not suitable\n", opcode);
#endif
                    ordinary_packet_cb(opcode, out_data, out_len, in_packet_len);
                }

            } else {
                // There is no pending reads, so data should be processed
                // as ordinary inbound packet

                // printf("ordinary_packet_cb 2\n");
#ifdef SKREE_NET_DEBUG
                printf("Got opcode: 0x%.2X, pending_reads is empty\n", opcode);
#endif
                ordinary_packet_cb(opcode, out_data, out_len, in_packet_len);
            }

            // TODO: get rid of memmove()
            read_queue_length -= in_packet_len;
            if(read_queue_length > 0)
                memmove(read_queue, read_queue + in_packet_len, read_queue_length);

            if(out_len > 0) {
                auto item = new Skree::Base::PendingWrite::QueueItem {
                    .len = out_len,
                    .data = out_data,
                    .pos = 0,
                    .cb = Skree::PendingReads::noop(server)
                };

                push_write_queue(item);
            }
        }
    }

    Client::Client(int _fh, struct ev_loop* _loop, sockaddr_in* _s_in, socklen_t _s_in_len, Server& _server)
        : fh(_fh), loop(_loop), s_in(_s_in), s_in_len(_s_in_len), server(_server) {
        read_queue = NULL;
        peer_name = NULL;
        peer_id = NULL;
        conn_name = NULL;
        conn_id = NULL;
        conn_port = 0;
        read_queue_length = 0;
        read_queue_mapped_length = 0;
        pthread_mutex_init(&write_queue_mutex, NULL);

        fcntl(fh, F_SETFL, fcntl(fh, F_GETFL, 0) | O_NONBLOCK);

        add_action_handler<Actions::W>();
        add_action_handler<Actions::L>();
        add_action_handler<Actions::E>();
        add_action_handler<Actions::R>();
        add_action_handler<Actions::C>();
        add_action_handler<Actions::I>();
        add_action_handler<Actions::X>();
        add_action_handler<Actions::H>();

        watcher.client = this;
        ev_io_init(&watcher.watcher, client_cb, fh, EV_READ | EV_WRITE);
        ev_io_start(loop, &watcher.watcher);
    }

    template<typename T>
    void Client::add_action_handler() {
        T* handler = new T(server, *this);
        handlers[handler->opcode()] = handler;
    }

    Client::~Client() {
        pthread_mutex_lock(&(server.known_peers_mutex));

        ev_io_stop(loop, &watcher.watcher);
        shutdown(fh, SHUT_RDWR);
        close(fh);
        free(s_in);

        if(peer_id != NULL) {
            known_peers_t::const_iterator known_peer = server.known_peers.find(peer_id);

            if(known_peer != server.known_peers.cend())
                server.known_peers.erase(known_peer);

            free(peer_id);
        }

        if(conn_id != NULL) {
            known_peers_t::const_iterator known_peer = server.known_peers_by_conn_id.find(conn_id);

            if(known_peer != server.known_peers_by_conn_id.cend())
                server.known_peers_by_conn_id.erase(known_peer);

            free(conn_id);
        }

        pthread_mutex_unlock(&(server.known_peers_mutex));

        while(!pending_reads.empty()) {
            auto item = pending_reads.front();

            auto _cb = item->cb;
            if(!_cb->noop()) {
                _cb->error(*this, *item);
            }

            pending_reads.pop_front();
        }

        while(!write_queue.empty()) {
            auto item = write_queue.front();
            write_queue.pop_front();

            auto _cb = item->cb->cb;
            if(!_cb->noop()) {
                _cb->error(*this, *(item->cb));
            }
        }

        pthread_mutex_destroy(&write_queue_mutex);

        if(read_queue != NULL) free(read_queue);
        if(peer_name != NULL) free(peer_name);
        if(conn_name != NULL) free(conn_name);
    }

    void Client::push_read_queue(size_t len, char* data) {
        size_t offset = read_queue_length;

        if(read_queue == NULL) {
            read_queue_length = len;
            read_queue_mapped_length = server.read_size +
                ((read_queue_length > server.read_size) ? read_queue_length : 0);

            read_queue = (char*)malloc(read_queue_mapped_length);

        } else {
            read_queue_length += len;

            if(read_queue_length > read_queue_mapped_length) {
                read_queue_mapped_length = server.read_size + read_queue_length;
                read_queue = (char*)realloc(read_queue, read_queue_length);
            }
        }

        memcpy(read_queue + offset, data, len);
        read_cb();

        return;
    }

    Skree::Base::PendingWrite::QueueItem* Client::get_pending_write() {
        pthread_mutex_lock(&write_queue_mutex);

        while(!write_queue.empty()) {
            auto item = write_queue.front();

            if((item->len > 0) && (item->pos < item->len)) {
                pthread_mutex_unlock(&write_queue_mutex);
                return item;
            }

            write_queue.pop_front();
            delete item;
        }

        ev_io_set(&watcher.watcher, fh, EV_READ);

        pthread_mutex_unlock(&write_queue_mutex);

        return NULL;
    }

    void Client::push_write_queue(
        Skree::Base::PendingWrite::QueueItem* item, bool front
    ) {
        //TODO: crutch
        char* new_msg = (char*)malloc(item->len + 4);
        new_msg[0] = item->data[0];
        uint32_t len = htonl(item->len - 1);
        memcpy(new_msg + 1, &len, 4);
        memcpy(new_msg + 5, item->data + 1, item->len - 1);
        item->len += 4;

        free(item->data);
        item->data = new_msg;

        pthread_mutex_lock(&write_queue_mutex);

        if(write_queue.empty())
            ev_io_set(&watcher.watcher, fh, EV_READ | EV_WRITE);

        if(front)
            write_queue.push_front(item);
        else
            write_queue.push_back(item);

        pthread_mutex_unlock(&write_queue_mutex);
    }

    void Client::push_pending_reads_queue(
        const Skree::Base::PendingRead::QueueItem* item, bool front
    ) {
        if(item->len == 0) {
            printf("Client::push_pending_reads_queue() got zero-length pending read, ignoring it\n");
            return;
        }

        // if(item->len == 9) {
        //     throw new std::logic_error ("trap");
        // }

        if(front)
            pending_reads.push_front(item);
        else
            pending_reads.push_back(item);
    }

    void Client::client_cb(struct ev_loop* loop, ev_io* _watcher, int events) {
        struct Utils::client_bound_ev_io* watcher = (struct Utils::client_bound_ev_io*)_watcher;
        Client*& client = watcher->client;

        if(events & EV_ERROR) {
            printf("EV_ERROR!\n");
            delete client;
            return;
        }

        if(events & EV_READ) {
            char* buf = (char*)malloc(client->server.read_size);
            int read = recv(_watcher->fd, buf, client->server.read_size, 0);

            if(read > 0) {
#ifdef SKREE_NET_DEBUG
                for(int i = 0; i < read; ++i)
                    printf("read from %s [%d]: 0x%.2X\n", client->get_peer_id(),i,buf[i]);
#endif
                client->push_read_queue(read, buf);
                free(buf);

            } else if(read < 0) {
                if((errno != EAGAIN) && (errno != EINTR)) {
                    perror("recv");
                    free(buf);
                    delete client;
                    return;
                }

            } else {
                free(buf);
                delete client;
                return;
            }
        }

        if(events & EV_WRITE) {
            auto item = client->get_pending_write();

            if(item != NULL) {
                int written = write(
                    _watcher->fd,
                    (item->data + item->pos),
                    (item->len - item->pos)
                );
#ifdef SKREE_NET_DEBUG
                printf("written %d bytes to %s/%s, len: %lu\n", written, client->get_peer_id(), client->get_conn_id(), item->len);
#endif
                if(written < 0) {
                    if((errno != EAGAIN) && (errno != EINTR)) {
                        perror("write");
                        delete client;
                        return;
                    }

                } else {
#ifdef SKREE_NET_DEBUG
                    for(int i = 0; i < written; ++i)
                        printf("written to %s [%d]: 0x%.2X\n", client->get_peer_id(),i,((char*)(item->data + item->pos))[i]);
#endif
                    item->pos += written;

                    if((item->pos >= item->len) && !item->cb->noop) {
                        client->push_pending_reads_queue(item->cb);
                        item->cb = Skree::PendingReads::noop(client->server);
                    }
                }
            }
        }
    }
}
