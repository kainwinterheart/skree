#include "client.hpp"
#include "meta.hpp"
// #include "base/pending_read.hpp"

namespace Skree {
    void Client::ordinary_packet_cb(const char& opcode, const size_t& in_packet_len) {
        if(handlers[opcode] == nullptr) {
            Utils::cluck(2, "Unknown packet header: 0x%.2X\n", opcode);

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
                    handlers[opcode]->in(item.len, args.data, args.out);
                    ++(server.stat_num_requests_detailed[opcode]);
                    return nullptr;
                };

                uint32_t _tmp;
                memcpy(&_tmp, args.data, sizeof(_tmp));
                _tmp = ntohl(_tmp);

#ifdef SKREE_NET_DEBUG
                Utils::cluck(2, "Need to read %u more bytes", _tmp);
#endif

                if(_tmp == 0) {
                    _cb(client, item, args);

                } else {
                    const auto cb = new Skree::PendingReads::Callbacks::OrdinaryPacket<decltype(_cb)>(server, _cb);
                    const auto _item = new Skree::Base::PendingRead::QueueItem {
                        .len = _tmp,
                        .cb = cb,
                        .ctx = nullptr,
                        .opcode = false,
                        .noop = false
                    };

                    push_pending_reads_queue(_item, true);
                }

                return nullptr;
            };

            const auto cb = new Skree::PendingReads::Callbacks::OrdinaryPacket<decltype(_cb)>(server, _cb);
            const auto item = new Skree::Base::PendingRead::QueueItem {
                .len = 4,
                .cb = cb,
                .ctx = nullptr,
                .opcode = false,
                .noop = false
            };

            push_pending_reads_queue(item, true);
        }
    }

    void Client::read_cb() {
        while(read_queue_length > 0) {
            size_t in_packet_len = 1;
            char opcode = read_queue[0];
            Skree::Base::PendingWrite::QueueItem* out = nullptr;

            if(!pending_reads.empty()) {
                // Pending reads queue is a top priority callback
                // If there is a pending read - incoming data should
                // be passed to such a callback
#ifdef SKREE_NET_DEBUG
                Utils::cluck(2, "Got opcode: 0x%.2X, pending_reads not empty\n", opcode);
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
                            const char* _data = args.data; // TODO?

                            // ***
                            Skree::Base::PendingRead::Callback::Args _args {
                                .data = _data,
                                .out = args.out,
                                .stop = args.stop,
                                .opcode = opcode
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
                            Utils::cluck(2, "About to run original callback with a packet of size %lu bytes\n", __item.len);
#endif
                            auto new_item = _cb->run(*this, __item, _args);

                            if(new_item != nullptr) {
                                push_write_queue(new_item);
                            }
                            // ***

                            return nullptr;
                        };

                        uint32_t _tmp;
                        memcpy(&_tmp, args.data, sizeof(_tmp));
                        _tmp = ntohl(_tmp);

                        if(_tmp == 0) {
                            const Skree::Base::PendingRead::QueueItem __item {
                                .len = 0,
                                .cb = nullptr,
                                .ctx = nullptr,
                                .opcode = false,
                                .noop = false
                            };

                            _cb(client, __item, args);

                        } else {
                            const auto cb = new Skree::PendingReads::Callbacks::OrdinaryPacket<decltype(_cb)>(server, _cb);
                            const auto __item = new Skree::Base::PendingRead::QueueItem {
                                .len = _tmp,
                                .cb = cb,
                                .ctx = nullptr,
                                .opcode = false,
                                .noop = false
                            };
#ifdef SKREE_NET_DEBUG
                            Utils::cluck(2, "Got opcode for pending read, need to read %lu more bytes\n", _tmp);
#endif
                            push_pending_reads_queue(__item, true);
                        }

                        return nullptr;
                    };

                    const auto cb = new Skree::PendingReads::Callbacks::OrdinaryPacket<decltype(_cb)>(server, _cb);
                    const auto item = new Skree::Base::PendingRead::QueueItem {
                        .len = 4,
                        .cb = cb,
                        .ctx = nullptr,
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
                        Utils::cluck(2, "Got raw pending read of size %lu bytes\n", item->len);
#endif
                        bool stop = false;
                        const char* _data = read_queue + in_packet_len;

                        pending_reads.pop_front();

                        // ***
                        Skree::Base::PendingRead::Callback::Args args {
                            .data = _data,
                            .out = out,
                            .stop = stop,
                            .opcode = '\0'
                        };

                        auto _cb = item->cb;
                        auto new_item = _cb->run(*this, *item, args);
                        in_packet_len += (item->len - in_packet_len);

                        if(new_item != nullptr) {
                            push_write_queue(new_item);
                        }
                        // ***

                        if(stop) {
                            // Utils::cluck(1, "pending read signaled to stop");
                            drop();
                            break;
                        }

                    } else {
                        // If we have a pending read, but requested amount
                        // of data is still not arrived - we should wait for it

                        break;
                    }

                } else if(protocol_version > 0) {
                    // If pending read waits for opcode, and it is not
                    // the reply opcode we've got here - process data as
                    // ordinary inbound packet

                    // Utils::cluck(1, "ordinary_packet_cb 1\n");
#ifdef SKREE_NET_DEBUG
                    Utils::cluck(2, "Got opcode: 0x%.2X, pending_reads is empty, but queue item is not suitable\n", opcode);
#endif
                    ordinary_packet_cb(opcode, in_packet_len);

                } else {
                    Utils::cluck(1, "invalid pending read");
                    drop();
                    break;
                }

            } else if(protocol_version > 0) {
                // There is no pending reads, so data should be processed
                // as ordinary inbound packet

                // Utils::cluck(1, "ordinary_packet_cb 2\n");
#ifdef SKREE_NET_DEBUG
                Utils::cluck(2, "Got opcode: 0x%.2X, pending_reads is empty\n", opcode);
#endif
                ordinary_packet_cb(opcode, in_packet_len);

            } else {
                Utils::cluck(1, "no pending reads and no protocol_version");
                drop();
                break;
            }

            // TODO: get rid of memmove()
            read_queue_length -= in_packet_len;
            if(read_queue_length > 0)
                memmove(read_queue, read_queue + in_packet_len, read_queue_length);

            if(out != nullptr) {
                out->finish();
                ++(server.stat_num_responses_detailed[out->get_opcode()]);
                push_write_queue(out);
            }
        }
    }

    Client::Client(int _fh, struct ev_loop* _loop, sockaddr_in* _s_in, socklen_t _s_in_len, Server& _server)
        : fh(_fh), loop(_loop), s_in(_s_in), s_in_len(_s_in_len), server(_server) {
        read_queue = nullptr;
        peer_name = nullptr;
        peer_id = nullptr;
        conn_name = nullptr;
        conn_id = nullptr;
        conn_port = 0;
        read_queue_length = 0;
        read_queue_mapped_length = 0;
        protocol_version = 0;
        destroyed = false;
        pthread_mutex_init(&write_queue_mutex, nullptr);

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
        destroy();
    }

    void Client::destroy() {
        bool _destroyed = destroyed.exchange(true);

        if(_destroyed)
            return;

        // if((peer_id != nullptr) || (conn_id != nullptr))
        //     Utils::cluck(3, "DROP(%s/%s)", peer_id, conn_id);

        auto& known_peers = server.known_peers;
        auto known_peers_end = known_peers.lock();

        ev_io_stop(loop, &watcher.watcher);
        shutdown(fh, SHUT_RDWR);
        close(fh);
        free(s_in);

        if(peer_id != nullptr) {
            auto known_peer = known_peers.find(peer_id);

            if(known_peer != known_peers_end) {
                auto& list = known_peer->second;
                Utils::RoundRobinVector<Skree::Client*> new_list;

                for(const auto& peer : list) {
                    if(this != peer) // TODO?
                        new_list.push_back(peer);
                }

                if(new_list.empty()) {
                    known_peers.erase(known_peer);
                    // Utils::cluck(2, "ERASE(known_peers.%s)", peer_id);

                } else {
                    list.swap(new_list);
                }
            }

            // free(peer_id); // TODO: it will break all other clients with the same peer_id
        }

        if(conn_id != nullptr) {
            auto& known_peers_by_conn_id = server.known_peers_by_conn_id;
            auto known_peers_by_conn_id_end = known_peers_by_conn_id.lock();
            auto known_peer = known_peers_by_conn_id.find(conn_id);

            if(known_peer != known_peers_by_conn_id_end) {
                // conn_id is NOT unique for outgoing connections
                auto& list = known_peer->second;
                Utils::RoundRobinVector<Skree::Client*> new_list;

                for(const auto& peer : list) {
                    if(this != peer) // TODO?
                        new_list.push_back(peer);
                }

                if(new_list.empty()) {
                    known_peers_by_conn_id.erase(known_peer);
                    // Utils::cluck(2, "ERASE(known_peers_by_conn_id.%s)", conn_id);

                } else {
                    list.swap(new_list);
                }
            }

            known_peers_by_conn_id.unlock();

            // free(conn_id); // TODO: it will break all other clients with the same peer_id
        }

        known_peers.unlock();

        while(!pending_reads.empty()) {
            auto item = pending_reads.front();

            auto _cb = item->cb;
            if(!_cb->noop()) {
                _cb->error(*this, *item);
            }

            pending_reads.pop_front();
        }

        pthread_mutex_lock(&write_queue_mutex);

        while(!write_queue.empty()) {
            auto item = write_queue.front();
            write_queue.pop_front();

            auto __cb = item->get_cb();

            if(__cb != nullptr) {
                auto _cb = __cb->cb;

                if(!_cb->noop())
                    _cb->error(*this, *__cb);
            }
        }

        pthread_mutex_unlock(&write_queue_mutex);
        pthread_mutex_destroy(&write_queue_mutex);

        if(read_queue != nullptr) free(read_queue);
        if(peer_name != nullptr) free(peer_name);
        if(conn_name != nullptr) free(conn_name);
    }

    void Client::push_read_queue(size_t len, char* data) {
        size_t offset = read_queue_length;

        if(read_queue == nullptr) {
            read_queue_length = len;
            read_queue_mapped_length = server.read_size +
                ((read_queue_length > server.read_size) ? read_queue_length : 0);

            read_queue = (char*)malloc(read_queue_mapped_length);

        } else {
            read_queue_length += len;

            if(read_queue_length > read_queue_mapped_length) {
                read_queue_mapped_length = server.read_size + read_queue_length;
                read_queue = (char*)realloc(read_queue, read_queue_mapped_length);
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

            if(item->can_be_written()) {
                pthread_mutex_unlock(&write_queue_mutex);
                return item;
            }

            write_queue.pop_front();
            delete item;
        }

        ev_io_set(&watcher.watcher, fh, EV_READ);

        pthread_mutex_unlock(&write_queue_mutex);

        return nullptr;
    }

    void Client::push_write_queue(
        Skree::Base::PendingWrite::QueueItem* item, bool front
    ) {
        item->finish();

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
            Utils::cluck(1, "Client::push_pending_reads_queue() got zero-length pending read, ignoring it\n");
            return;
        }

        // if(item->len == 9) {
        //     throw std::logic_error ("trap");
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
            Utils::cluck(1, "EV_ERROR!\n");
            client->drop();
            return;
        }

        if(events & EV_READ) {
            char* buf = (char*)malloc(client->server.read_size);
            int read = recv(_watcher->fd, buf, client->server.read_size, 0);

            if(read > 0) {
#ifdef SKREE_NET_DEBUG
                for(int i = 0; i < read; ++i)
                    fprintf(stderr, "read from %s/%s [%d]: 0x%.2X\n", client->get_peer_id(),client->get_conn_id(),i,((unsigned char*)buf)[i]);
#endif
                client->push_read_queue(read, buf);
                free(buf);

            } else if(read < 0) {
                if((errno != EAGAIN) && (errno != EINTR)) {
                    perror("recv");
                    free(buf);
                    client->drop();
                    return;
                }

            } else {
                if(client->get_peer_id() != nullptr)
                    Utils::cluck(3, "%s/%s disconnected", client->get_peer_id(), client->get_conn_id());

                free(buf);
                client->drop();
                return;
            }
        }

        if(events & EV_WRITE) {
            auto item = client->get_pending_write();

            if(item != nullptr)
                item->write(*client, _watcher->fd);
        }
    }

    void Client::drop() {
        destroy();
        // delete this; // TODO: does this break ReplicationPingTask somehow?
    }
}
