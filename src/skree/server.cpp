#include "server.hpp"
#include "meta.hpp"

namespace Skree {
    Server::Server(
        uint32_t _my_port,
        uint32_t _max_client_threads,
        uint32_t _max_parallel_connections,
        const Utils::known_events_t& _known_events
    )
      : my_port(_my_port)
      , max_client_threads(_max_client_threads)
      , max_parallel_connections(_max_parallel_connections)
      , known_events(_known_events)
    {
        pthread_mutex_init(&new_clients_mutex, nullptr);

        stat_num_inserts = 0;
        stat_num_replications = 0;
        stat_num_repl_it = 0;

        for(unsigned char i = 0; i <= 255;) {
            stat_num_requests_detailed[i] = 0;
            stat_num_responses_detailed[i] = 0;

            if(i < 255) {
                ++i;

            } else if(i == 255) {
                break;
            }
        }

        load_peers_to_discover();

        my_hostname = (char*)"127.0.0.1";
        my_hostname_len = strlen(my_hostname);
        my_peer_id = Utils::make_peer_id(my_hostname_len, my_hostname, my_port);
        my_peer_id_len = strlen(my_peer_id);
        my_peer_id_len_net = htonl(my_peer_id_len);

        sockaddr_in addr;

        int fh = socket(PF_INET, SOCK_STREAM, 0);

        addr.sin_family = AF_UNSPEC;
        addr.sin_port = htons(my_port);
        addr.sin_addr.s_addr = INADDR_ANY;

        int yes = 1;

        if(setsockopt(fh, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
            perror("setsockopt");
            throw std::runtime_error("Socket error");
        }

        if(bind(fh, (sockaddr*)&addr, sizeof(addr)) != 0) {
            perror("bind");
            throw std::runtime_error("Socket error");
        }

        fcntl(fh, F_SETFL, fcntl(fh, F_GETFL, 0) | O_NONBLOCK);
        listen(fh, 100000);

        Utils::server_bound_ev_io socket_watcher;
        socket_watcher.server = this;
        struct ev_loop* loop = ev_loop_new(0);

        ev_io_init((ev_io*)&socket_watcher, socket_cb, fh, EV_READ);
        ev_io_start(loop, (ev_io*)&socket_watcher);

        Skree::Workers::Synchronization synchronization (*this);
        synchronization.start();

        for(int i = 0; i < max_client_threads; ++i) {
            auto client = new Skree::Workers::Client(*this);
            threads.push(client);
            client->start();
        }

        {
            peer_to_discover_t* localhost7654 = (peer_to_discover_t*)malloc(
                sizeof(*localhost7654));

            localhost7654->host = "127.0.0.1";
            localhost7654->port = 7654;

            peer_to_discover_t* localhost8765 = (peer_to_discover_t*)malloc(
                sizeof(*localhost8765));

            localhost8765->host = "127.0.0.1";
            localhost8765->port = 8765;

            peer_to_discover_t* localhost9876 = (peer_to_discover_t*)malloc(
                sizeof(*localhost9876));

            localhost9876->host = "127.0.0.1";
            localhost9876->port = 9876;

            peers_to_discover[Utils::make_peer_id(
                strlen(localhost7654->host),
                localhost7654->host,
                localhost7654->port

            )] = localhost7654;

            peers_to_discover[Utils::make_peer_id(
                strlen(localhost8765->host),
                localhost8765->host,
                localhost8765->port

            )] = localhost8765;

            peers_to_discover[Utils::make_peer_id(
                strlen(localhost9876->host),
                localhost9876->host,
                localhost9876->port

            )] = localhost9876;
        }

        Skree::Workers::Replication replication (*this);
        replication.start();

        Skree::Workers::Discovery discovery (*this);
        discovery.start();

        Skree::Workers::Processor processor (*this);
        processor.start();

        ev_run(loop, 0); // TODO
    }

    Server::~Server() {
        while(!threads.empty()) {
            auto thread = threads.front();
            threads.pop();
            delete thread; // TODO
        }

        pthread_mutex_destroy(&new_clients_mutex);
    }

    short Server::repl_save(
        in_packet_r_ctx* ctx,
        Client& client,
        QueueDb& queue
    ) {
        const uint32_t _peers_cnt = htonl(ctx->peers_count);
        auto* serialized_peers = new Skree::Utils::StringSequence (
            sizeof(_peers_cnt),
            (const char*)&_peers_cnt
        );
        decltype(serialized_peers) serialized_peers_last = serialized_peers;

        packet_r_ctx_peer* peer;
        char* _peer_id;
        // bool keep_peer_id;
        // uint64_t _peer_id_len;
        peers_to_discover_t::iterator prev_item;
        peers_to_discover_t::iterator peers_to_discover_end;
        bool _save_peers_to_discover = false;

        for(uint32_t i = 0; i < ctx->peers_count; ++i) {
            peer = ctx->peers[i];
            _peer_id = Utils::make_peer_id(peer->hostname_len, peer->hostname, peer->port);

            // keep_peer_id = false;
            // _peer_id_len = strlen(_peer_id);

            peers_to_discover_end = peers_to_discover.lock();
            prev_item = peers_to_discover.find(_peer_id);

            if(prev_item == peers_to_discover_end) {
                peers_to_discover[_peer_id] = new peer_to_discover_t {
                    .host = strdup(peer->hostname),
                    .port = peer->port
                };

                // keep_peer_id = true;
                _save_peers_to_discover = true;
            }

            peers_to_discover.unlock();

            auto* next = new Skree::Utils::StringSequence (
                strlen(_peer_id) + 1,
                _peer_id
            );

            serialized_peers_last->concat(next);
            serialized_peers_last = next;

            // TODO
            // if(!keep_peer_id) {
            //     free(_peer_id);
            //     free(peer->hostname);
            // }

            delete peer;
        }

        if(_save_peers_to_discover)
            save_peers_to_discover();

        uint64_t now = htonll(std::time(nullptr));
        size_t now_len = sizeof(now);
        in_packet_r_ctx_event* event;
        uint32_t event_len;
        uint32_t _hostname_len = htonl(ctx->hostname_len);
        uint32_t _port = htonl(ctx->port);
        auto peer_id = Utils::make_peer_id(ctx->hostname_len, ctx->hostname, ctx->port);

        char failover_key [
            strlen(peer_id)
            + 1 // :
            + 20 // wrinseq
            + 1 // \0
        ];

        auto& db = *(queue.kv);
        uint32_t processed = 0;

        for(uint32_t i = 0; i < ctx->events_count; ++i) {
            event = ctx->events[i];
            event_len = htonl(event->len);

            // TODO: ntohll(event->id_net) -> event->id
            sprintf(failover_key, "%s:%llu", peer_id, ntohll(event->id_net));

            if(db.add(failover_key, strlen(failover_key), "0", 1)) {
                auto stream = queue.write();

                stream->write(sizeof(event_len), &event_len);
                stream->write(event->len, event->data);
                stream->write(now_len, &now);
                stream->write(sizeof(event->id_net), &(event->id_net)); // == rid
                stream->write(sizeof(_hostname_len), &_hostname_len);
                stream->write(ctx->hostname_len, ctx->hostname);
                stream->write(sizeof(_port), &_port);
                stream->write(serialized_peers);

                delete stream;

                ++processed;
                ++stat_num_replications;

            } else {
                Utils::cluck(3, "[repl_save] db.add(%s) failed: %s\n", failover_key, db.error().name());
                break;
            }

            // free(event->data); // TODO
            delete event;
        }

        delete serialized_peers;
        // free(ctx->hostname); // TODO

        return ((processed == ctx->events_count) ? REPL_SAVE_RESULT_K : REPL_SAVE_RESULT_F);
    }

    // TODO: get rid of ctx
    short Server::save_event(
        in_packet_e_ctx* ctx,
        uint32_t replication_factor,
        Client* client,
        uint64_t* task_ids,
        QueueDb& queue
    ) {
        if(replication_factor > max_replication_factor)
            replication_factor = max_replication_factor;

        const char* _event_name = ctx->event_name;
        auto r_req = Actions::R::out_init(
            *this,
            ctx->event_name_len,
            _event_name,
            ctx->cnt
        );

        short result = SAVE_EVENT_RESULT_F;
        bool replication_began = false;
        auto& db = *(queue.kv);
        int64_t max_id = db.increment("inseq", 5, ctx->cnt, 0);

        if(max_id == kyotocabinet::INT64MIN) {
            Utils::cluck(2, "Increment failed: %s\n", db.error().name());

        } else {
            uint32_t _cnt = 0;
            uint32_t num_inserted = 0;
            const char* _event_data;
            max_id -= ctx->cnt;
            uint64_t _max_id;

            while(_cnt < ctx->cnt) {
                ++max_id;
                in_packet_e_ctx_event* event = ctx->events[_cnt];
                ++_cnt;

                // TODO: is this really necessary?
                // event->id = (char*)malloc(21);
                // sprintf(event->id, "%lu", max_id);

                _max_id = htonll(max_id);

                // TODO: insert all events in a transaction
                if(db.add((char*)&_max_id, sizeof(_max_id), "0", 1)) {
                    auto stream = queue.write();
                    stream->write(sizeof(_max_id), &_max_id);
                    stream->write(event->len, event->data);
                    delete stream;

                    if(task_ids != nullptr)
                        task_ids[_cnt] = max_id;

                    ++num_inserted;
                    ++stat_num_inserts;

                    _event_data = event->data; // TODO
                    r_req = Actions::R::out_add_event(r_req, max_id, event->len, _event_data);

                    // {
                    //     size_t sz;
                    //     char* val = db.get((char*)&_max_id, sizeof(_max_id), &sz);
                    //     Utils::cluck(2, "value size: %zu\n", sz);
                    //     if(sz > 0) {
                    //         Utils::cluck(2, "value: %s\n", val);
                    //     }
                    // }

                } else {
                    Utils::cluck(3, "[save_event] db.add(%llu) failed: %s\n", max_id, db.error().name());
                    break;
                }
            }

            r_req->finish();

            if(num_inserted == ctx->cnt) {
                /*****************************/
                std::vector<char*>* candidate_peer_ids = new std::vector<char*>();
                auto accepted_peers = new std::list<packet_r_ctx_peer*>();

                known_peers.lock();
// Utils::cluck(2, "REPLICATION ATTEMPT: %lu\n", known_peers.size());
                for(auto& it : known_peers) {
                    candidate_peer_ids->push_back(it.first);
                }

                known_peers.unlock();

                // TODO
                std::random_shuffle(
                    candidate_peer_ids->begin(),
                    candidate_peer_ids->end()
                );

                auto r_ctx = new out_packet_r_ctx {
                    .sync = (replication_factor > 0),
                    .replication_factor = replication_factor,
                    .pending = 0,
                    .client = client,
                    .candidate_peer_ids = candidate_peer_ids,
                    .accepted_peers = accepted_peers,
                    .r_req = r_req
                };
                /*****************************/

                if(r_ctx->sync) {
                    if(candidate_peer_ids->size() > 0) {
                        result = SAVE_EVENT_RESULT_NULL;

                        begin_replication(r_ctx);
                        replication_began = true;

                    } else {
                        result = SAVE_EVENT_RESULT_A;
                        delete r_ctx;
                    }

                } else {
                    result = SAVE_EVENT_RESULT_K;

                    if(candidate_peer_ids->size() > 0) {
                        begin_replication(r_ctx);
                        replication_began = true;

                    } else {
                        delete r_ctx;
                    }
                }

            } else {
                Utils::cluck(1, "Batch insert failed\n");
            }
        }

        if(!replication_began) delete r_req;

        return result;
    }

    void Server::repl_clean(
        size_t failover_key_len,
        const char* failover_key,
        Utils::known_event_t& event
    ) {
        auto& kv = *(event.r_queue->kv);

        if(!kv.remove(failover_key, failover_key_len)) {
            Utils::cluck(3, "Key %s could not be removed: %s\n", failover_key, kv.error().name());
        }
    }

    void Server::begin_replication(out_packet_r_ctx*& r_ctx) {
        Client* peer = nullptr;

        while((peer == nullptr) && (r_ctx->candidate_peer_ids->size() > 0)) {
            char* peer_id = r_ctx->candidate_peer_ids->back();
            r_ctx->candidate_peer_ids->pop_back();

            auto known_peers_end = known_peers.lock();
            auto it = known_peers.find(peer_id);
            known_peers.unlock();

            if(it != known_peers_end)
                peer = it->second.next();
        }

        bool done = false;

        if(peer == nullptr) {
            if(r_ctx->sync) {
                uint32_t accepted_peers_count = r_ctx->accepted_peers->size();

                if(accepted_peers_count >= r_ctx->replication_factor) {
                    if(r_ctx->client != nullptr) {
                        r_ctx->client->push_write_queue(new Skree::Base::PendingWrite::QueueItem(
                            0, SKREE_META_OPCODE_K
                        ));
                    }

                    r_ctx->sync = false;

                } else if(r_ctx->pending == 0) {
                    if(r_ctx->client != nullptr) {
                        r_ctx->client->push_write_queue(new Skree::Base::PendingWrite::QueueItem(
                            0, SKREE_META_OPCODE_A
                        ));
                    }

                    r_ctx->sync = false;
                }
            }

            if(r_ctx->pending == 0)
                done = true;

        } else {
            uint32_t accepted_peers_count = r_ctx->accepted_peers->size();

            if(
                r_ctx->sync
                && (accepted_peers_count >= r_ctx->replication_factor)
            ) {
                if(r_ctx->client != nullptr) {
                    r_ctx->client->push_write_queue(new Skree::Base::PendingWrite::QueueItem(
                        0, SKREE_META_OPCODE_K
                    ));
                }

                r_ctx->sync = false;
            }

            if(accepted_peers_count >= max_replication_factor) {
                done = true;

            } else {
                auto out = new Skree::Base::PendingWrite::QueueItem(
                    r_ctx->r_req, sizeof(accepted_peers_count)
                );

                uint32_t _accepted_peers_count = htonl(accepted_peers_count);
                out->copy_concat(sizeof(_accepted_peers_count), &_accepted_peers_count);

                for(
                    std::list<packet_r_ctx_peer*>::const_iterator it =
                        r_ctx->accepted_peers->cbegin();
                    it != r_ctx->accepted_peers->cend();
                    ++it
                ) {
                    packet_r_ctx_peer* peer = *it;

                    // out->grow(sizeof(peer->hostname_len) + peer->hostname_len + sizeof(peer->port));

                    uint32_t _len = htonl(peer->hostname_len);
                    out->copy_concat(sizeof(_len), &_len);

                    out->concat(peer->hostname_len + 1, peer->hostname);
                    out->copy_concat(sizeof(peer->port), &(peer->port));
                }

                ++(r_ctx->pending);

                out->set_cb(new Skree::Base::PendingRead::QueueItem {
                    .cb = new Skree::PendingReads::Callbacks::Replication(*this),
                    .ctx = (void*)r_ctx
                });

                out->finish();
                peer->push_write_queue(out);
            }
        }

        if(done) {
            while(!r_ctx->accepted_peers->empty()) {
                packet_r_ctx_peer* peer = r_ctx->accepted_peers->back();
                r_ctx->accepted_peers->pop_back();
                free(peer);
            }

            free(r_ctx->accepted_peers);
            free(r_ctx->candidate_peer_ids);
            delete r_ctx->r_req;
            free(r_ctx);
        }
    }

    void Server::save_peers_to_discover() {
        peers_to_discover.lock();

        uint32_t cnt = htonl(peers_to_discover.size());
        uint32_t dump_len = 0;
        char* dump = (char*)malloc(sizeof(cnt));

        memcpy(dump + dump_len, &cnt, sizeof(cnt));
        dump_len += sizeof(cnt);

        peer_to_discover_t* peer;
        size_t len;
        uint32_t port;
        size_t _len;

        for(auto it : peers_to_discover) {
            peer = it.second;

            len = strlen(peer->host);
            port = htonl(peer->port);

            dump = (char*)realloc(dump,
                dump_len
                + sizeof(len)
                + len
                + sizeof(port)
            );

            _len = htonll(len);
            memcpy(dump + dump_len, &_len, sizeof(_len));
            dump_len += sizeof(_len);

            memcpy(dump + dump_len, peer->host, len);
            dump_len += len;

            memcpy(dump + dump_len, &port, sizeof(port));
            dump_len += sizeof(port);
        }

        peers_to_discover.unlock();

        const char* key = "peers_to_discover";
        const size_t key_len = strlen(key);

        // TODO
        // if(!db.set(key, key_len, dump, dump_len))
        //     Utils::cluck(2, "Failed to save peers list: %s\n", db.error().name());
    }

    void Server::load_peers_to_discover() {
        const char* key = "peers_to_discover";
        const size_t key_len = strlen(key);
        size_t value_len;

        // TODO
        char* value = nullptr;//db.get(key, key_len, &value_len);

        if(value != nullptr) {
            uint32_t offset = 0;
            uint32_t cnt (ntohl(*(uint64_t*)(value + offset)));
            offset += sizeof(cnt);

            uint32_t hostname_len;
            char* hostname;
            uint32_t port;
            char* peer_id;
            peers_to_discover_t::iterator it;
            peers_to_discover_t::iterator peers_to_discover_end;
            peer_to_discover_t* peer;

            while(cnt > 0) {
                --cnt;

                hostname_len = ntohl(*(uint32_t*)(value + offset));
                offset += sizeof(hostname_len);

                hostname = (char*)malloc(hostname_len + 1);
                memcpy(hostname, value + offset, hostname_len);
                hostname[hostname_len] = '\0'; // TODO
                offset += hostname_len;

                port = ntohl(*(uint32_t*)(value + offset));
                offset += sizeof(port);

                peer_id = Utils::make_peer_id(hostname_len, hostname, port);

                peers_to_discover_end = peers_to_discover.lock();
                it = peers_to_discover.find(peer_id);

                if(it == peers_to_discover_end) {
                    peer = (peer_to_discover_t*)malloc(sizeof(*peer));

                    peer->host = hostname;
                    peer->port = port;

                    peers_to_discover[peer_id] = peer;

                } else {
                    free(peer_id);
                    free(hostname);
                }

                peers_to_discover.unlock();
            }
        }
    }

    void Server::socket_cb(struct ev_loop* loop, ev_io* _watcher, int events) {
        struct Utils::server_bound_ev_io* watcher = (struct Utils::server_bound_ev_io*)_watcher;
        auto server = watcher->server;
        sockaddr_in* addr = (sockaddr_in*)malloc(sizeof(*addr));
        socklen_t len = sizeof(*addr);

        int fh = accept(_watcher->fd, (sockaddr*)addr, &len);

        if(fh < 0) {
            perror("accept");
            free(addr);
            return;
        }

        new_client_t* new_client = new new_client_t {
            .fh = fh,
            .cb = [server](Client& client){ // TODO: also in Discovery::on_new_client
                client.push_write_queue(Skree::Actions::N::out_init(), true);
            },
            .s_in = addr,
            .s_in_len = len
        };

        // TODO
        pthread_mutex_lock(&(server->new_clients_mutex));
        server->new_clients.push(new_client);
        pthread_mutex_unlock(&(server->new_clients_mutex));
    }

    void Server::replication_exec(out_packet_i_ctx* ctx) {
        // Utils::cluck(2, "Replication exec for task %lu\n", ctx->rid);

        if(*(ctx->acceptances) == *(ctx->count_replicas)) {
            in_packet_e_ctx_event event {
                .len = ctx->data->len,
                .data = ctx->data->data,
                .id = nullptr
            };

            in_packet_e_ctx_event* events [1];
            events[0] = &event;

            in_packet_e_ctx e_ctx {
                .cnt = 1,
                .event_name_len = ctx->event->id_len,
                .event_name = ctx->event->id,
                .events = events
            };

            uint64_t task_ids[1];
            save_event(&e_ctx, 0, nullptr, task_ids, *(ctx->event->queue));

            // TODO: remove?
            // {
            //     in_packet_e_ctx* ctx = (in_packet_e_ctx*)_ctx;
            //
            //     for(
            //         std::list<in_packet_e_ctx_event*>::const_iterator it = ctx->events->cbegin();
            //         it != ctx->events->cend();
            //         ++it
            //     ) {
            //         in_packet_e_ctx_event* event = *it;
            //
            //         free(event->data);
            //         if(event->id != nullptr) free(event->id);
            //         free(event);
            //     }
            //
            //     free(ctx->event_name);
            //     free(ctx->events);
            //     free(ctx);
            // }

            auto& failover = ctx->event->failover;
            failover.lock();
            failover[ctx->failover_key] = task_ids[0];
            failover.unlock();

            // TODO: this should probably make 'i' packet return 'f' // TODO: why? // oh, okay
            repl_clean(
                ctx->failover_key_len,
                ctx->failover_key,
                *(ctx->event)
            );

            if(ctx->rpr != nullptr) {
                // const muh_str_t*& peer_id, const known_event_t*& event,
                // const uint64_t& rid
                auto x_req = Skree::Actions::X::out_init(ctx->peer_id, *(ctx->event), ctx->rid);
                size_t offset = 0;
                bool written = false;

                while(ctx->peers_cnt > 0) {
                    size_t peer_id_len = strlen(ctx->rpr + offset);
                    char* peer_id = ctx->rpr + offset;
                    offset += peer_id_len + 1;

                    auto known_peers_end = known_peers.lock();
                    auto it = known_peers.find(peer_id);
                    known_peers.unlock();

                    if(it != known_peers_end) {
                        it->second.next()->push_write_queue(x_req); // TODO: it will be freed after FIRST successfull write
                        written = true;
                    }

                    --(ctx->peers_cnt);
                }

                if(!written)
                    delete x_req;
            }

        } else {
            // TODO: think about repl_clean()
            repl_clean(
                ctx->failover_key_len,
                ctx->failover_key,
                *(ctx->event)
            );

            ctx->event->unfailover(ctx->failover_key);

            // free(ctx->data->data);
            delete ctx->data;
        }

        pthread_mutex_destroy(ctx->mutex);

        free(ctx->mutex);
        free(ctx->acceptances);
        free(ctx->pending);
        free(ctx->count_replicas);
        free(ctx);
    }

    short Server::get_event_state(
        uint64_t id,
        Utils::known_event_t& event,
        const uint64_t now
    ) {
        auto wip_end = wip.lock();
        auto it = wip.find(id);

        if(it != wip_end) {
            // TODO: check for overflow
            if((it->second + job_time) > now) {
                wip.unlock();
                return SKREE_META_EVENTSTATE_PROCESSING; // It is ok to wait

            } else {
                wip.erase(it); // TODO: this should not be here
                wip.unlock();
                return SKREE_META_EVENTSTATE_LOST; // TODO: this could possibly flap
            }
        }

        wip.unlock();

        auto& kv = *(event.queue->kv);
        uint64_t id_net = htonll(id);

        size_t flag_size;
        // TODO: this could possibly flap too
        char* flag = kv.get((char*)&id_net, sizeof(id_net), &flag_size);

        if(flag_size < 1) {
            return SKREE_META_EVENTSTATE_PROCESSED;

        } else if((flag_size == 1) && (flag[0] == '0')) {
            return SKREE_META_EVENTSTATE_PENDING;

        } else {
            return SKREE_META_EVENTSTATE_LOST;
        }
    }
}
