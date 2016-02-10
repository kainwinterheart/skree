#include "client.hpp"

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

                PendingReadsQueueItem** _item = pending_reads.front();
                PendingReadsQueueItem* item = *_item;

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

                        PendingReadCallbackArgs args = {
                            .data = read_queue + in_packet_len,
                            .len = item->len,
                            .out_data = &out_data,
                            .out_len = &out_len,
                            .stop = &stop,
                            .ctx = item->ctx
                        };

                        PendingReadsQueueItem* new_item = (this ->* (item->cb))(&args);
                        in_packet_len += item->len;

                        free(item);

                        if(new_item == nullptr) {
                            // If pending read has received all its data -
                            // remove it from the queue

                            pending_reads.pop_front();
                            free(_item);

                        } else {
                            // If pending read requests more data -
                            // wait for it

                            *_item = new_item;
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

    PendingReadsQueueItem* Client::discovery_cb9(PendingReadCallbackArgs* args) {
        uint32_t host_len = args->len - 4;

        char* host = (char*)malloc(host_len + 1);
        memcpy(host, args->data, host_len);
        host[host_len] = '\0';

        uint32_t _port;
        memcpy(&_port, args->data + host_len, 4);
        uint32_t port = ntohl(_port);

        char* _peer_id = make_peer_id(host_len, host, port);

        pthread_mutex_lock(&peers_to_discover_mutex);

        peers_to_discover_t::const_iterator prev_item = peers_to_discover.find(_peer_id);

        if(prev_item == peers_to_discover.cend()) {
            peer_to_discover_t* peer_to_discover = (peer_to_discover_t*)malloc(
                sizeof(*peer_to_discover));

            peer_to_discover->host = host;
            peer_to_discover->port = port;

            peers_to_discover[_peer_id] = peer_to_discover;
            save_peers_to_discover();

        } else {
            free(_peer_id);
            free(host);
        }

        pthread_mutex_unlock(&peers_to_discover_mutex);

        uint32_t prev_len = args->len;
        args->len = 4;

        char* prev_data = args->data;
        args->data = (char*)(args->ctx);

        PendingReadsQueueItem* rv = discovery_cb7(args);

        args->len = prev_len;
        args->data = prev_data;

        return rv;
    }

    static void Client::free_discovery_ctx(void* _ctx) {
        uint32_t* ctx = (uint32_t*)_ctx;

        free(ctx);
    }

    PendingReadsQueueItem* Client::discovery_cb8(PendingReadCallbackArgs* args) {
        uint32_t _len;
        memcpy(&_len, args->data, args->len);
        uint32_t len = ntohl(_len);

        PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
            sizeof(*item));

        item->len = len + 4;
        item->cb = &Client::discovery_cb9;
        item->ctx = args->ctx;
        item->err = &Client::free_discovery_ctx;
        item->opcode = false;

        return item;
    }

    PendingReadsQueueItem* Client::discovery_cb7(PendingReadCallbackArgs* args) {
        uint32_t _cnt;
        memcpy(&_cnt, args->data, args->len);
        uint32_t cnt = ntohl(_cnt);

        if(cnt > 0) {
            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof(*item));

            item->len = 4;
            item->cb = &Client::discovery_cb8;
            item->opcode = false;

            _cnt = htonl(cnt - 1);

            if(args->ctx == NULL) {
                uint32_t* ctx = (uint32_t*)malloc(sizeof(*ctx));
                memcpy(ctx, &_cnt, args->len);

                item->ctx = (void*)ctx;
                item->err = &Client::free_discovery_ctx;

            } else {
                memcpy(args->ctx, &_cnt, args->len);

                item->ctx = args->ctx;
                item->err = &Client::free_discovery_ctx;
            }

            return item;

        } else {
            if(args->ctx != NULL)
                free(args->ctx);

            return nullptr;
        }
    }

    PendingReadsQueueItem* Client::discovery_cb6(PendingReadCallbackArgs* args) {
        if(args->data[0] == SKREE_META_OPCODE_K) {
            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof(*item));

            item->len = 4;
            item->cb = &Client::discovery_cb7;
            item->ctx = NULL;
            item->err = NULL;
            item->opcode = false;

            return item;

        } else {
            return nullptr;
        }
    }

    PendingReadsQueueItem* Client::discovery_cb5(PendingReadCallbackArgs* args) {
        if(args->data[0] == SKREE_META_OPCODE_K) {
            pthread_mutex_lock(&known_peers_mutex);

            // peer_id is guaranteed to be set here
            known_peers_t::const_iterator known_peer = known_peers.find(peer_id);

            if(known_peer == known_peers.cend()) {
                known_peers[peer_id] = this;
                known_peers_by_conn_id[get_conn_id()] = this;

            } else {
                *(args->stop) = true;
            }

            pthread_mutex_unlock(&known_peers_mutex);

            if(!*(args->stop)) {
                auto l_req = Actions::L::out_init();

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof(*item));

                item->len = 1;
                item->cb = &Client::discovery_cb6;
                item->ctx = NULL;
                item->err = NULL;
                item->opcode = true;

                push_write_queue(l_req->len, l_req->data, item);
            }

            return nullptr;

        } else {
            *(args->stop) = true;
        }

        return nullptr;
    }

    PendingReadsQueueItem* Client::discovery_cb4(PendingReadCallbackArgs* args) {
        uint32_t port = get_conn_port();
        char* _peer_id = make_peer_id(args->len, args->data, port);
        bool accepted = false;

        pthread_mutex_lock(&known_peers_mutex);

        known_peers_t::const_iterator known_peer = known_peers.find(_peer_id);

        if(known_peer == known_peers.cend()) {
            if(strcmp(_peer_id, my_peer_id) == 0) {
                pthread_mutex_lock(&me_mutex);

                me_t::const_iterator it = me.find(_peer_id);

                if(it == me.cend()) {
                    char* _conn_peer_id = get_conn_id();

                    me[_peer_id] = true;
                    me[strdup(_conn_peer_id)] = true;

                } else {
                    free(_peer_id);
                }

                pthread_mutex_unlock(&me_mutex);

            } else {
                peer_name = (char*)malloc(args->len);
                memcpy(peer_name, args->data, args->len);

                peer_name_len = args->len;
                peer_port = port;
                peer_id = _peer_id;

                accepted = true;
            }

        } else {
            free(_peer_id);
        }

        pthread_mutex_unlock(&known_peers_mutex);

        if(accepted) {
            size_t h_len = 0;
            auto h_req = Actions::H::out_init(server);

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof(*item));

            item->len = 1;
            item->cb = &Client::discovery_cb5;
            item->ctx = NULL;
            item->err = NULL;
            item->opcode = true;

            push_write_queue(h_req->len, h_req->data, item);

        } else {
            *(args->stop) = true;
        }

        return nullptr;
    }

    PendingReadsQueueItem* Client::discovery_cb3(PendingReadCallbackArgs* args) {
        uint32_t _len;
        memcpy(&_len, args->data, args->len);
        uint32_t len = ntohl(_len);

        PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
            sizeof(*item));

        item->len = len;
        item->cb = &Client::discovery_cb4;
        item->ctx = NULL;
        item->err = NULL;
        item->opcode = false;

        return item;
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
            PendingReadsQueueItem** _item = pending_reads.front();
            PendingReadsQueueItem* item = *_item;

            if(item->ctx != NULL) {
                if(item->err == NULL) {
                    fprintf(stderr, "Don't known how to free pending read context\n");

                } else {
                    item->err(item->ctx);
                }
            }

            free(item);
            free(_item);

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
    void Client::push_write_queue(size_t len, char* data, PendingReadsQueueItem* cb) {
        WriteQueueItem* item = (WriteQueueItem*)malloc(sizeof(*item));

        item->len = len;
        item->data = data;
        item->pos = 0;
        item->cb = cb;

        pthread_mutex_lock(&write_queue_mutex);

        if(write_queue.empty())
            ev_io_set(&watcher.watcher, fh, EV_READ | EV_WRITE);

        write_queue.push(item);

        pthread_mutex_unlock(&write_queue_mutex);
    }

    void Client::push_pending_reads_queue(PendingReadsQueueItem* item, bool front = false) {
        PendingReadsQueueItem** _item = (PendingReadsQueueItem**)malloc(
            sizeof(*_item));
        *_item = item;

        if(front)
            pending_reads.push_front(_item);
        else
            pending_reads.push_back(_item);
    }

    // TODO: should go to Skree::Workers::Discovery
    PendingReadsQueueItem* Client::discovery_cb2(PendingReadCallbackArgs* args) {
        if(args->data[0] == SKREE_META_OPCODE_K) {
            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof(*item));

            item->len = 4;
            item->cb = &Client::discovery_cb3;
            item->ctx = NULL;
            item->err = NULL;
            item->opcode = false;

            return item;

        } else {
            *(args->stop) = true;

            return nullptr;
        }
    }

    PendingReadsQueueItem* Client::replication_cb(PendingReadCallbackArgs* args) {
        out_packet_r_ctx* ctx = (out_packet_r_ctx*)(args->ctx);
        --(ctx->pending);

        if(args->data[0] == SKREE_META_OPCODE_K) {
            packet_r_ctx_peer* peer =
                (packet_r_ctx_peer*)malloc(sizeof(*peer));

            peer->hostname_len = get_peer_name_len();
            peer->hostname = get_peer_name();
            peer->port = htonl(get_peer_port());

            ctx->accepted_peers->push_back(peer);
        }

        server->begin_replication(ctx);

        return nullptr;
    }

    static void Client::replication_skip_peer(void* _ctx) {
        out_packet_r_ctx* ctx = (out_packet_r_ctx*)_ctx;
        --(ctx->pending);

        server->begin_replication(ctx);
    }

    PendingReadsQueueItem* Client::propose_self_k_cb(PendingReadCallbackArgs* args) {
        out_data_i_ctx* ctx = (out_data_i_ctx*)(args->ctx);

        pthread_mutex_lock(ctx->mutex);

        --(*(ctx->pending));

        if(args->data[0] == SKREE_META_OPCODE_K)
            ++(*(ctx->acceptances));

        continue_replication_exec(ctx);

        pthread_mutex_unlock(ctx->mutex);

        return nullptr;
    }

    static void Client::propose_self_f_cb(void* _ctx) {
        out_data_i_ctx* ctx = (out_data_i_ctx*)_ctx;

        pthread_mutex_lock(ctx->mutex);

        --(*(ctx->pending));

        continue_replication_exec(ctx);

        pthread_mutex_unlock(ctx->mutex);
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

    PendingReadsQueueItem* Client::ping_task_k_cb(PendingReadCallbackArgs* args) {
        out_data_c_ctx* ctx = (out_data_c_ctx*)(args->ctx);

        if(args->data[0] == SKREE_META_OPCODE_K) {
            repl_clean(
                ctx->failover_key_len,
                ctx->failover_key,
                ctx->wrinseq
            );

        } else {
            ping_task_f_cb((void*)ctx);
        }

        unfailover(ctx->failover_key);

        // pthread_mutex_lock(ctx->mutex);
        //
        // pthread_mutex_unlock(ctx->mutex);

        return nullptr;
    }

    static void Client::ping_task_f_cb(void* _ctx) {
        out_data_c_ctx* ctx = (out_data_c_ctx*)_ctx;
        in_packet_r_ctx* r_ctx = (in_packet_r_ctx*)malloc(sizeof(*r_ctx));

        r_ctx->hostname_len = ctx->client->get_peer_name_len();
        r_ctx->port = ctx->client->get_peer_port();
        r_ctx->hostname = strndup(
            ctx->client->get_peer_name(),
            r_ctx->hostname_len
        );
        r_ctx->event_name_len = ctx->event->id_len;
        r_ctx->event_name = strndup(
            ctx->event->id,
            r_ctx->event_name_len
        );
        r_ctx->events = new std::list<in_packet_r_ctx_event*>();
        r_ctx->cnt = 0;
        r_ctx->peers = new std::list<packet_r_ctx_peer*>();

        in_packet_r_ctx_event* event = (in_packet_r_ctx_event*)malloc(
            sizeof(*event));

        event->len = ctx->rin->len;
        event->data = ctx->rin->data;
        event->id = (char*)malloc(21);
        sprintf(event->id, "%llu", ctx->rid);

        r_ctx->events->push_back(event);

        if(ctx->rpr != NULL) {
            size_t rpr_len = ctx->rpr->len;
            size_t rpr_offset = 0;

            while(rpr_offset < rpr_len) {
                size_t peer_id_len = strlen(ctx->rpr->data + rpr_offset);
                char* peer_id = (char*)malloc(peer_id_len + 1);
                memcpy(peer_id, ctx->rpr->data + rpr_offset, peer_id_len);
                peer_id[peer_id_len] = '\0';
                rpr_offset += peer_id_len + 1;

                char* delimiter = rindex(peer_id, ':');

                if(delimiter == NULL) {
                    fprintf(stderr, "Invalid peer id: %s\n", peer_id);

                } else {
                    packet_r_ctx_peer* peer = (packet_r_ctx_peer*)malloc(
                        sizeof(*peer));

                    peer->hostname_len = delimiter - peer_id;
                    peer->port = atoi(delimiter + 1);
                    peer->hostname = (char*)malloc(peer->hostname_len + 1);

                    memcpy(peer->hostname, peer_id, peer->hostname_len);
                    peer->hostname[peer->hostname_len] = '\0';

                    r_ctx->peers->push_back(peer);
                }
            }
        }

        short result = repl_save(r_ctx, ctx->client);

        if(result == REPL_SAVE_RESULT_K) {
            repl_clean(
                ctx->failover_key_len,
                ctx->failover_key,
                ctx->wrinseq
            );

        } else if(result == REPL_SAVE_RESULT_F) {
            fprintf(stderr, "repl_save() failed\n");
            exit(1);

        } else {
            fprintf(stderr, "Unexpected repl_save() result: %d\n", result);
            exit(1);
        }

        unfailover(ctx->failover_key);

        // pthread_mutex_lock(ctx->mutex);
        //
        // pthread_mutex_unlock(ctx->mutex);
    }
}
