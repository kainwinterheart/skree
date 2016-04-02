#include "client.hpp"
#include "base/pending_read.hpp"

namespace Skree {
    void Client::ordinary_packet_cb(
        const char& opcode, char** out_data,
        size_t* out_len, size_t* in_packet_len
    ) {
        handlers_t::const_iterator it = handlers.find(opcode);

        if(it == handlers.cend()) {
            printf("Unknown packet header: 0x%.2X\n", opcode);

        } else {
            // TODO: change protocol
            // in(uint64_t in_len, char* in_data,
            // uint64_t* out_len, char** out_data)
            // TODO: it->second->in();
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

                const Skree::Base::PendingRead::QueueItem&& item (std::move(pending_reads.front()));

                if(
                    (
                        (item.opcode == true)
                        && (
                            (opcode == SKREE_META_OPCODE_K) || (opcode == SKREE_META_OPCODE_F)
                            || (opcode == SKREE_META_OPCODE_A)
                        )
                    )
                    || (item.opcode == false)
                ) {
                    // If pending read waits for opcode, and it is the
                    // reply opcode we've got here, or if pending read
                    // does not wait for opcode - process pending read

                    --in_packet_len;

                    if(read_queue_length >= item.len) {
                        bool stop = false;

                        Skree::Base::PendingRead::Callback::Args args = {
                            .data = read_queue + in_packet_len,
                            .out_data = out_data,
                            .out_len = out_len,
                            .stop = stop
                        };

                        const Skree::Base::PendingRead::QueueItem&& new_item (item.run(this, item, args));
                        in_packet_len += item.len;

                        pending_reads.pop_front();

                        if(!new_item.noop) {
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
                    ordinary_packet_cb(opcode, &out_data, &out_len,
                        &in_packet_len);
                }

            } else {
                // There is no pending reads, so data should be processed
                // as ordinary inbound packet

                // printf("ordinary_packet_cb 2\n");
                ordinary_packet_cb(opcode, &out_data, &out_len,
                    &in_packet_len);
            }

            read_queue_length -= in_packet_len;
            if(read_queue_length > 0)
                memmove(read_queue, read_queue + in_packet_len, read_queue_length);

            if(out_len > 0)
                push_write_queue(out_len, out_data, NULL);
        }
    }

    static void Client::free_discovery_ctx(void* _ctx) {
        uint32_t* ctx = (uint32_t*)_ctx;

        free(ctx);
    }

    Client::Client(int _fh, struct ev_loop* _loop, sockaddr_in* _s_in, socklen_t _s_in_len)
        : fh(_fh), loop(_loop), s_in(_s_in), s_in_len(_s_in_len) {
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
        T* handler = new T(server, this);
        handlers[handler->opcode] = handler;
    }

    Client::~Client() {
        pthread_mutex_lock(&known_peers_mutex);

        ev_io_stop(loop, &watcher.watcher);
        shutdown(fh, SHUT_RDWR);
        close(fh);
        free(s_in);

        if(peer_id != NULL) {
            known_peers_t::const_iterator known_peer = known_peers.find(peer_id);

            if(known_peer != known_peers.cend())
                known_peers.erase(known_peer);

            free(peer_id);
        }

        if(conn_id != NULL) {
            known_peers_t::const_iterator known_peer = known_peers_by_conn_id.find(conn_id);

            if(known_peer != known_peers_by_conn_id.cend())
                known_peers_by_conn_id.erase(known_peer);

            free(conn_id);
        }

        pthread_mutex_unlock(&known_peers_mutex);

        while(!pending_reads.empty()) {
            const Skree::Base::PendingRead::QueueItem item (std::move(pending_reads.front()));

            if(!item.cb.noop()) {
                item.cb.error(this, item);
            }

            pending_reads.pop_front();
        }

        while(!write_queue.empty()) {
            WriteQueueItem* item = write_queue.front();
            write_queue.pop();

            if(item->cb != NULL) {
                if(item->cb->ctx != NULL) {
                    if(item->cb->err == NULL) {
                        fprintf(stderr, "Don't known how to free pending read context\n");

                    } else {
                        item->cb->err(item->cb->ctx);
                    }
                }

                free(item->cb);
            }

            free(item->data);
            free(item);
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
            read_queue_mapped_length = read_size +
                ((read_queue_length > read_size) ? read_queue_length : 0);

            read_queue = (char*)malloc(read_queue_mapped_length);

        } else {
            read_queue_length += len;

            if(read_queue_length > read_queue_mapped_length) {
                read_queue_mapped_length = read_size + read_queue_length;
                read_queue = (char*)realloc(read_queue, read_queue_length);
            }
        }

        memcpy(read_queue + offset, data, len);
        read_cb();

        return;
    }

    WriteQueueItem* Client::get_pending_write() {
        WriteQueueItem* result = NULL;

        pthread_mutex_lock(&write_queue_mutex);

        while(!write_queue.empty()) {
            WriteQueueItem* item = write_queue.front();

            if((item->len > 0) && (item->pos < item->len)) {
                result = item;
                break;

            } else {
                write_queue.pop();
                free(item->data);
                if(item->cb != NULL) free(item->cb);
                free(item);
            }
        }

        if(result == NULL)
            ev_io_set(&watcher.watcher, fh, EV_READ);

        pthread_mutex_unlock(&write_queue_mutex);

        return result;
    }

    // TODO: use muh_str_t instead of 'len' and 'data'
    void Client::push_write_queue(size_t len, char* data, const Skree::Base::PendingRead::QueueItem& cb) {
        WriteQueueItem* item = (WriteQueueItem*)malloc(sizeof(*item));

        item->len = len;
        item->data = data;
        item->pos = 0;
        item->cb = std::move(cb);

        pthread_mutex_lock(&write_queue_mutex);

        if(write_queue.empty())
            ev_io_set(&watcher.watcher, fh, EV_READ | EV_WRITE);

        write_queue.push(item);

        pthread_mutex_unlock(&write_queue_mutex);
    }

    void Client::push_pending_reads_queue(const Skree::Base::PendingRead::QueueItem& item, bool front = false) {
        if(front)
            pending_reads.push_front(std::move(item));
        else
            pending_reads.push_back(std::move(item));
    }

    static void Client::free_in_packet_e_ctx(void* _ctx) {
        in_packet_e_ctx* ctx = (in_packet_e_ctx*)_ctx;

        for(
            std::list<in_packet_e_ctx_event*>::const_iterator it = ctx->events->cbegin();
            it != ctx->events->cend();
            ++it
        ) {
            in_packet_e_ctx_event* event = *it;

            free(event->data);
            if(event->id != NULL) free(event->id);
            free(event);
        }

        free(ctx->event_name);
        free(ctx->events);
        free(ctx);
    }

    static void client_cb(struct ev_loop* loop, ev_io* _watcher, int events) {
        struct client_bound_ev_io* watcher = (struct client_bound_ev_io*)_watcher;
        Client*& client = watcher->client;

        if(events & EV_ERROR) {
            printf("EV_ERROR!\n");
            delete client;
            return;
        }

        if(events & EV_READ) {
            char* buf = (char*)malloc(read_size);
            int read = recv(_watcher->fd, buf, read_size, 0);

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
            WriteQueueItem* item = client->get_pending_write();

            if(item != NULL) {
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

                    if((item->pos >= item->len) && (item->cb != NULL)) {
                        client->push_pending_reads_queue(item->cb);
                        item->cb = NULL;
                    }
                }
            }
        }
    }
}
