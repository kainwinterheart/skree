#include "client.hpp"
// #include "base/pending_read.hpp"

namespace Skree {
    void Client::ordinary_packet_cb(
        const char& opcode, char*& out_data,
        size_t& out_len, const size_t& in_packet_len
    ) {
        if(handlers[opcode] == NULL) {
            printf("Unknown packet header: 0x%.2X\n", opcode);

        } else {
            // TODO: should be one pending read instead of two
            auto _cb = [this, &opcode](
                const Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item,
                const Skree::Base::PendingRead::Callback::Args& args
            ) {
                auto _cb = [this, &opcode](
                    const Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item,
                    const Skree::Base::PendingRead::Callback::Args& args
                ) {
                    uint64_t _out_len; // TODO: git rid of this
                    handlers[opcode]->in(item.len, args.data, _out_len, args.out_data);
                    args.out_len = _out_len;

                    return Skree::PendingReads::noop(server);
                };

                const auto cb = new Skree::PendingReads::Callbacks::OrdinaryPacket<decltype(_cb)>(server, _cb);
                uint32_t _tmp;

                memcpy(&_tmp, args.data, sizeof(_tmp));

                const auto _item = new Skree::Base::PendingRead::QueueItem {
                    .len = ntohl(_tmp),
                    .cb = cb,
                    .ctx = NULL,
                    .opcode = false,
                    .noop = false
                };

                return _item;
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

                auto item = pending_reads.front();

                if(
                    (
                        (item->opcode == true)
                        && (
                            (opcode == SKREE_META_OPCODE_K) || (opcode == SKREE_META_OPCODE_F)
                            || (opcode == SKREE_META_OPCODE_A)
                        )
                    )
                    || (item->opcode == false)
                ) {
                    // If pending read waits for opcode, and it is the
                    // reply opcode we've got here, or if pending read
                    // does not wait for opcode - process pending read

                    --in_packet_len;

                    if(read_queue_length >= item->len) {
                        bool stop = false;

                        const char* _data = read_queue + in_packet_len;

                        Skree::Base::PendingRead::Callback::Args args = {
                            .data = _data,
                            .out_data = out_data,
                            .out_len = out_len,
                            .stop = stop
                        };

                        auto _cb = item->cb;
                        auto new_item = _cb->run(*this, *item, args);
                        in_packet_len += item->len;

                        pending_reads.pop_front();

                        if(!new_item->noop) {
                            push_pending_reads_queue(new_item, true);
                        }

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
                    ordinary_packet_cb(opcode, out_data, out_len, in_packet_len);
                }

            } else {
                // There is no pending reads, so data should be processed
                // as ordinary inbound packet

                // printf("ordinary_packet_cb 2\n");
                ordinary_packet_cb(opcode, out_data, out_len, in_packet_len);
            }

            read_queue_length -= in_packet_len;
            if(read_queue_length > 0)
                memmove(read_queue, read_queue + in_packet_len, read_queue_length);

            if(out_len > 0) {
                auto item = new Skree::Base::PendingWrite::QueueItem {
                    .len = out_len,
                    .data = out_data,
                    .pos = 0,
                    .cb = Skree::PendingReads::noop(server),
                    .noop = false
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

        // TODO?
        auto item = new Skree::Base::PendingWrite::QueueItem {
            .len = 0,
            .pos = 0,
            .cb = Skree::PendingReads::noop(server),
            .data = NULL,
            .noop = true
        };

        return item;
    }

    void Client::push_write_queue(
        Skree::Base::PendingWrite::QueueItem* item, bool front
    ) {
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
                // for(int i = 0; i < read; ++i)
                //     printf("read from %s: 0x%.2X\n", client->get_peer_id(),buf[i]);

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

            if(!item->noop) {
                int written = write(
                    _watcher->fd,
                    (item->data + item->pos),
                    (item->len - item->pos)
                );

                if(written < 0) {
                    if((errno != EAGAIN) && (errno != EINTR)) {
                        perror("write");
                        delete client;
                        return;
                    }

                } else {
                    // for(int i = 0; i < written; ++i)
                    //     printf("written to %s: 0x%.2X\n", client->get_peer_id(),((char*)(item->data + item->pos))[i]);

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
