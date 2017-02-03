#include "server.hpp"
#include "meta.hpp"
#include <ctime>

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
        my_peer_id_len_net = htonl(my_peer_id->len);

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

        Skree::Workers::Cleanup cleanup (*this);
        cleanup.start();

        Skree::Workers::Synchronization synchronization (*this);
        synchronization.start();

        Skree::Workers::Statistics statistics (*this);
        statistics.start();

        for(int i = 0; i < max_client_threads; ++i) {
            auto args = std::make_shared<Skree::Workers::Client::Args>();
            auto client = std::make_shared<Skree::Workers::Client>(*this, args.get());

            threads.push_back(std::make_pair(args, client));

            client->start();
        }

        // Debug
        {
            {
                std::shared_ptr<Utils::muh_str_t> host;
                host.reset(new Utils::muh_str_t {
                    .own = false,
                    .len = 9,
                    .data = (char*)"127.0.0.1"
                });

                std::shared_ptr<peer_to_discover_t> peer;
                peer.reset(new peer_to_discover_t {
                    .host = host,
                    .port = 7654
                });

                peers_to_discover[Utils::make_peer_id(
                    peer->host->len,
                    peer->host->data,
                    peer->port
                )] = peer;
            }

            {
                std::shared_ptr<Utils::muh_str_t> host;
                host.reset(new Utils::muh_str_t {
                    .own = false,
                    .len = 9,
                    .data = (char*)"127.0.0.1"
                });

                std::shared_ptr<peer_to_discover_t> peer;
                peer.reset(new peer_to_discover_t {
                    .host = host,
                    .port = 8765
                });

                peers_to_discover[Utils::make_peer_id(
                    peer->host->len,
                    peer->host->data,
                    peer->port
                )] = peer;
            }

            {
                std::shared_ptr<Utils::muh_str_t> host;
                host.reset(new Utils::muh_str_t {
                    .own = false,
                    .len = 9,
                    .data = (char*)"127.0.0.1"
                });

                std::shared_ptr<peer_to_discover_t> peer;
                peer.reset(new peer_to_discover_t {
                    .host = host,
                    .port = 9876
                });

                peers_to_discover[Utils::make_peer_id(
                    peer->host->len,
                    peer->host->data,
                    peer->port
                )] = peer;
            }
        }

        Skree::Workers::Replication replication (*this);
        replication.start();

        Skree::Workers::Discovery discovery (*this);
        discovery.start();

        Skree::Workers::Processor processor (*this);
        processor.start();

        while(true) { // TODO
            NMuhEv::TLoop loop;
            auto list = NMuhEv::MakeEvList(1);

            loop.AddEvent(NMuhEv::TEvSpec {
                .Ident = (uintptr_t)fh,
                .Filter = NMuhEv::MUHEV_FILTER_READ,
                .Flags = NMuhEv::MUHEV_FLAG_NONE,
                .Ctx = nullptr
            }, list);

            int triggeredCount = loop.Wait(list);
            // Utils::cluck(2, "%d", triggeredCount);
            if(triggeredCount < 0) {
                perror("kevent");
                abort();

            } else if(triggeredCount > 0) {
                for(int i = 0; i < triggeredCount; ++i) {
                    socket_cb(NMuhEv::GetEvent(list, i));
                }
            }
        }
    }

    Server::~Server() {
    }

    short Server::repl_save(
        in_packet_r_ctx& ctx,
        Client& client,
        QueueDb& queue
    ) {
        bool _save_peers_to_discover = false;
        size_t serialized_peers_len = 0;

        for(uint32_t i = 0; i < ctx.peers_count; ++i) {
            const auto peer = (*ctx.peers.get())[i];
            auto _peer_id = Utils::make_peer_id(peer->hostname->len, peer->hostname->data, peer->port);

            auto peers_to_discover_end = peers_to_discover.lock();
            auto prev_item = peers_to_discover.find(_peer_id);

            if(prev_item == peers_to_discover_end) {
                peers_to_discover[_peer_id].reset(new peer_to_discover_t {
                    .host = peer->hostname,
                    .port = peer->port
                });

                _save_peers_to_discover = true;
            }

            peers_to_discover.unlock();

            serialized_peers_len +=
                sizeof(peer->hostname->len)
                + peer->hostname->len
                + sizeof(peer->port);
        }

        if(_save_peers_to_discover)
            save_peers_to_discover();

        const uint64_t now = htonll(std::time(nullptr));
        const size_t now_len = sizeof(now);
        const uint32_t _hostname_len = htonl(ctx.hostname_len);
        const uint32_t _port = htonl(ctx.port);
        auto peer_id = Utils::make_peer_id(ctx.hostname_len, ctx.hostname, ctx.port);

        char failover_key [
            peer_id->len
            + 1 // :
            + 20 // wrinseq
            + 1 // 'm'
            + 1 // \0
        ];

        auto& db = *(queue.kv);
        uint32_t processed = 0;
        const uint32_t _peers_cnt = htonl(ctx.peers_count);

        {
            auto kv_batch = db.NewSession();
            auto queue_batch = db.NewSession(DbWrapper::TSession::ST_QUEUE);

            for(uint32_t i = 0; i < ctx.events_count; ++i) {
                const auto event = (*ctx.events.get())[i];
                const uint32_t event_len = htonl(event->len);

                size_t data_offset = 0;
                char* data = (char*)malloc(
                    sizeof(event_len)
                    + event->len
                    + now_len
                    + sizeof(event->id_net)
                    + sizeof(_hostname_len)
                    + ctx.hostname_len
                    + sizeof(_port)
                    + sizeof(_peers_cnt)
                    + serialized_peers_len
                );

                memcpy(data + data_offset, &event_len, sizeof(event_len));
                data_offset += sizeof(event_len);

                memcpy(data + data_offset, event->data, event->len);
                data_offset += event->len;

                memcpy(data + data_offset, &now, now_len);
                data_offset += now_len;

                memcpy(data + data_offset, &(event->id_net), sizeof(event->id_net)); // == rid
                data_offset += sizeof(event->id_net);

                memcpy(data + data_offset, &_hostname_len, sizeof(_hostname_len));
                data_offset += sizeof(_hostname_len);

                memcpy(data + data_offset, ctx.hostname, ctx.hostname_len);
                data_offset += ctx.hostname_len;

                memcpy(data + data_offset, &_port, sizeof(_port));
                data_offset += sizeof(_port);

                memcpy(data + data_offset, &_peers_cnt, sizeof(_peers_cnt));
                data_offset += sizeof(_peers_cnt);

                // memcpy(data + data_offset, serialized_peers); // TODO
                for(uint32_t i = 0; i < ctx.peers_count; ++i) {
                    const auto peer = (*ctx.peers.get())[i];
                    const uint32_t hostname_len = htonl(peer->hostname->len);
                    const uint32_t port = htonl(peer->port);

                    memcpy(data + data_offset, &hostname_len, sizeof(hostname_len));
                    data_offset += sizeof(hostname_len);

                    memcpy(data + data_offset, peer->hostname->data, peer->hostname->len);
                    data_offset += peer->hostname->len;

                    memcpy(data + data_offset, &port, sizeof(port));
                    data_offset += sizeof(port);
                }

                uint64_t key;
                bool rv = queue_batch->append(&key, data, data_offset);

                free(data); // TODO: according to WT docs, this could break insertions

                if(rv) {
                    sprintf(failover_key, "%s:%llu", peer_id->data, key);
                    size_t failover_key_len = strlen(failover_key);

                    kv_batch->add(failover_key, failover_key_len, "0", 1);

                    failover_key[failover_key_len] = 'm';
                    ++failover_key_len;
                    auto keyNet = htonll(key);

                    kv_batch->add(
                        failover_key,
                        failover_key_len,
                        (char*)&keyNet,
                        sizeof(keyNet)
                    );

                    ++processed;
                    ++stat_num_replications;

                } else {
                    Utils::cluck(1, "WOOT");
                    abort();
                }

                // free(event->data); // TODO
                // delete event; // TODO?
            }
        }

        // free(ctx.hostname); // TODO

        return ((processed == ctx.events_count) ? REPL_SAVE_RESULT_K : REPL_SAVE_RESULT_F);
    }

    // TODO: get rid of ctx
    short Server::save_event(
        in_packet_e_ctx& ctx,
        uint32_t replication_factor,
        std::shared_ptr<Client> client,
        uint64_t* task_ids,
        QueueDb& queue
    ) {
        if(replication_factor > max_replication_factor)
            replication_factor = max_replication_factor;

        short result = SAVE_EVENT_RESULT_F;
        bool replication_began = false;
        auto& db = *(queue.kv);

        uint32_t num_inserted = 0;
        uint32_t msg_len = sizeof(ctx.event_name_len) + ctx.event_name_len + 1 + sizeof(ctx.cnt);

        while(num_inserted < ctx.cnt) {
            const auto& event = (*ctx.events.get())[num_inserted];
            msg_len += sizeof(uint64_t) + sizeof(event->len) + event->len;
            ++num_inserted;
        }

        num_inserted = 0;
        char* msg = (char*)malloc(msg_len);
        uint32_t msg_pos = 0;
        auto _msg = std::shared_ptr<void>((void*)msg, [](void* msg) {
            free((char*)msg);
        });

        const uint32_t _event_name_len = htonl(ctx.event_name_len);
        memcpy(msg + msg_pos, &_event_name_len, sizeof(_event_name_len));
        msg_pos += sizeof(_event_name_len);

        memcpy(msg + msg_pos, ctx.event_name, ctx.event_name_len + 1);
        msg_pos += ctx.event_name_len + 1;

        const uint32_t _cnt = htonl(ctx.cnt);
        memcpy(msg + msg_pos, &_cnt, sizeof(_cnt));
        msg_pos += sizeof(_cnt);

        {
            auto kv_batch = db.NewSession();
            auto queue_batch = db.NewSession(DbWrapper::TSession::ST_QUEUE);

            while(num_inserted < ctx.cnt) {
                auto event = (*ctx.events.get())[num_inserted];

                // TODO: insert all events in a transaction
                uint64_t key;
                bool rv = queue_batch->append(&key, event->data, event->len);

                // free(data); // TODO: according to WT docs, this could break insertions

                if(rv) {
                    uint64_t _key = htonll(key);
                    rv = kv_batch->add((char*)&_key, sizeof(_key), "0", 1);
                }

                if(rv) {
                    if(task_ids != nullptr) {
                        // Utils::cluck(3, "add task_id: %u, %llu", num_inserted, key);
                        task_ids[num_inserted] = key;
                    }

                    ++num_inserted;
                    ++stat_num_inserts;

                    const auto _key = htonll(key);
                    memcpy(msg + msg_pos, &_key, sizeof(_key));
                    msg_pos += sizeof(_key);

                    const auto _len = htonl(event->len);
                    memcpy(msg + msg_pos, &_len, sizeof(_len));
                    msg_pos += sizeof(_len);

                    memcpy(msg + msg_pos, event->data, event->len);
                    msg_pos += event->len;
                }
            }
        }

        auto r_req = std::make_shared<Skree::Base::PendingWrite::QueueItem>(Skree::Actions::R::opcode());
        r_req->concat(msg_len, msg);
        r_req->finish();

        if(num_inserted == ctx.cnt) {
            /*****************************/
            auto candidate_peer_ids = std::make_shared<std::vector<std::shared_ptr<Utils::muh_str_t>>>();
            auto accepted_peers = std::make_shared<std::list<std::shared_ptr<packet_r_ctx_peer>>>();

            known_peers.lock();
// Utils::cluck(2, "REPLICATION ATTEMPT: %lu\n", known_peers.size());
            for(auto& it : known_peers) {
                candidate_peer_ids->push_back(it.first);
            }

            known_peers.unlock();

            if(candidate_peer_ids->size() > 0) {
                // TODO
                std::random_shuffle(
                    candidate_peer_ids->begin(),
                    candidate_peer_ids->end()
                );
            }

            std::shared_ptr<out_packet_r_ctx> r_ctx;
            r_ctx.reset(new out_packet_r_ctx {
                .sync = (replication_factor > 0),
                .replication_factor = replication_factor,
                .pending = 0,
                .client = client,
                .candidate_peer_ids = candidate_peer_ids,
                .accepted_peers = accepted_peers,
                .r_req = r_req,
                .origin = _msg
            });
            /*****************************/

            if(r_ctx->sync) {
                if(candidate_peer_ids->size() > 0) {
                    result = SAVE_EVENT_RESULT_NULL;

                    begin_replication(r_ctx);
                    replication_began = true;

                } else {
                    result = SAVE_EVENT_RESULT_A;
                    // delete r_ctx;
                }

            } else {
                result = SAVE_EVENT_RESULT_K;

                if(candidate_peer_ids->size() > 0) {
                    begin_replication(r_ctx);
                    replication_began = true;

                // } else {
                //     delete r_ctx;
                }
            }

        } else {
            Utils::cluck(1, "Batch insert failed\n");
        }

        // if(!replication_began) delete r_req;

        return result;
    }

    void Server::repl_clean(
        size_t failover_key_len,
        const char* failover_key,
        Utils::known_event_t& event
    ) {
        auto& kv = *(event.r_queue->kv);
        char meta_key [failover_key_len + 2];

        memcpy(meta_key, failover_key, failover_key_len);
        meta_key[failover_key_len] = 'm';
        meta_key[failover_key_len + 1] = '0';

        const auto& meta = kv.get(meta_key, failover_key_len + 1);

        if(meta.size() > 0) {
            uint64_t id;
            memcpy(&id, meta.data(), sizeof(id));
            id = ntohll(id);

            if(!kv.remove(id)) {
                Utils::cluck(2, "Key %llu could not be removed", id);
            }
        }

        if(!kv.remove(meta_key, failover_key_len + 1)) {
            Utils::cluck(2, "Key %s could not be removed", meta_key);
        }

        if(!kv.remove(failover_key, failover_key_len)) {
            Utils::cluck(2, "Key %s could not be removed", failover_key);
        }
    }

    void Server::begin_replication(std::shared_ptr<out_packet_r_ctx> r_ctx) {
        Client* peer = nullptr; // TODO: should be shared_ptr<Client> 'cos segfault is possible

        while((peer == nullptr) && (r_ctx->candidate_peer_ids->size() > 0)) {
            const auto& peer_id = r_ctx->candidate_peer_ids->back();

            auto known_peers_end = known_peers.lock();
            auto it = known_peers.find(peer_id);
            known_peers.unlock();

            if(it != known_peers_end)
                peer = it->second.next();

            r_ctx->candidate_peer_ids->pop_back();
        }

        bool done = false;

        if(peer == nullptr) {
            if(r_ctx->sync) {
                uint32_t accepted_peers_count = r_ctx->accepted_peers->size();

                if(accepted_peers_count >= r_ctx->replication_factor) {
                    if(r_ctx->client) {
                        r_ctx->client->push_write_queue(std::make_shared<Skree::Base::PendingWrite::QueueItem>(
                            SKREE_META_OPCODE_K
                        ));
                    }

                    r_ctx->sync = false;

                } else if(r_ctx->pending == 0) {
                    if(r_ctx->client) {
                        r_ctx->client->push_write_queue(std::make_shared<Skree::Base::PendingWrite::QueueItem>(
                            SKREE_META_OPCODE_A
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
                if(r_ctx->client) {
                    r_ctx->client->push_write_queue(std::make_shared<Skree::Base::PendingWrite::QueueItem>(
                        SKREE_META_OPCODE_K
                    ));
                }

                r_ctx->sync = false;
            }

            if(accepted_peers_count >= max_replication_factor) {
                done = true;

            } else {
                auto out = std::make_shared<Skree::Base::PendingWrite::QueueItem>(r_ctx->r_req);

                uint32_t _accepted_peers_count = htonl(accepted_peers_count);
                out->copy_concat(sizeof(_accepted_peers_count), &_accepted_peers_count);

                for(const auto& peer : *r_ctx->accepted_peers) {
                    // out->grow(sizeof(peer->hostname_len) + peer->hostname_len + sizeof(peer->port));

                    uint32_t _len = htonl(peer->hostname->len);
                    out->copy_concat(sizeof(_len), &_len);

                    out->concat(peer->hostname->len + 1, peer->hostname->data);
                    out->copy_concat(sizeof(peer->port), &peer->port);
                }

                ++(r_ctx->pending);

                std::shared_ptr<void> _r_ctx (r_ctx, (void*)r_ctx.get());

                out->set_cb(std::make_shared<Skree::Base::PendingRead::QueueItem>(
                    _r_ctx, std::make_shared<Skree::PendingReads::Callbacks::Replication>(*this)
                ));

                out->finish();
                peer->push_write_queue(out);
            }
        }

        // if(done) {
            // while(!r_ctx->accepted_peers->empty()) {
            //     packet_r_ctx_peer* peer = r_ctx->accepted_peers->back();
            //     r_ctx->accepted_peers->pop_back();
            //     free(peer);
            // }

            // free(r_ctx->accepted_peers);
            // free(r_ctx->candidate_peer_ids);
            // delete r_ctx->r_req;
            // free(r_ctx);
        // }
    }

    void Server::save_peers_to_discover() {
        peers_to_discover.lock();

        uint32_t cnt = htonl(peers_to_discover.size());
        uint32_t dump_len = 0;
        char* dump = (char*)malloc(sizeof(cnt));

        memcpy(dump + dump_len, &cnt, sizeof(cnt));
        dump_len += sizeof(cnt);

        for(auto it : peers_to_discover) {
            const auto peer = it.second;

            uint32_t port = htonl(peer->port);

            dump = (char*)realloc(dump,
                dump_len
                + sizeof(peer->host->len)
                + peer->host->len
                + sizeof(port)
            );

            uint32_t _len = htonl(peer->host->len);
            memcpy(dump + dump_len, &_len, sizeof(_len));
            dump_len += sizeof(_len);

            memcpy(dump + dump_len, peer->host->data, peer->host->len);
            dump_len += peer->host->len;

            memcpy(dump + dump_len, &port, sizeof(port));
            dump_len += sizeof(port);
        }

        peers_to_discover.unlock();

        const char* key = "peers_to_discover";
        const size_t key_len = strlen(key);

        // TODO
        // if(!db.set(key, key_len, dump, dump_len))
        //     Utils::cluck(2, "Failed to save peers list: %s\n", db.error().name());
        free(dump);
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

            while(cnt > 0) {
                --cnt;

                auto hostname_len = ntohl(*(uint32_t*)(value + offset));
                offset += sizeof(hostname_len);

                auto hostname = Utils::NewStr(hostname_len + 1);
                --hostname->len;

                memcpy(hostname->data, value + offset, hostname_len);
                hostname->data[hostname_len] = '\0'; // TODO
                offset += hostname_len;

                auto port = ntohl(*(uint32_t*)(value + offset));
                offset += sizeof(port);

                auto peer_id = Utils::make_peer_id(hostname_len, hostname->data, port);

                auto peers_to_discover_end = peers_to_discover.lock();
                auto it = peers_to_discover.find(peer_id);

                if(it == peers_to_discover_end) {
                    peers_to_discover[peer_id].reset(new peer_to_discover_t {
                        .host = hostname,
                        .port = port
                    });

                // } else {
                    // free(peer_id);
                    // free(hostname);
                }

                peers_to_discover.unlock();
            }
        }
    }

    void Server::socket_cb(const NMuhEv::TEvSpec& event) {
        auto addr = std::make_shared<sockaddr_in>();
        socklen_t len = sizeof(sockaddr_in);

        int fh = accept(event.Ident, (sockaddr*)addr.get(), &len);

        if(fh < 0) {
            perror("accept");
            // free(addr);
            return;
        }

        std::shared_ptr<new_client_t> new_client;
        new_client.reset(new new_client_t {
            .fh = fh,
            .cb = [](Client& client){ // TODO: also in Discovery::on_new_client
                client.push_write_queue(Skree::Actions::N::out_init(), true);
            },
            .s_in = addr
        });

        push_new_client(new_client);
    }

    void Server::push_new_client(std::shared_ptr<new_client_t> new_client) {
        auto thread = threads.next();

        while(thread.first->mutex.exchange(true)) {
            continue;
        }

        thread.first->queue.push(new_client);

        if(!thread.first->mutex.exchange(false)) {
            abort();
        }

        // Utils::cluck(1, "push_new_client()");
        ::write(thread.first->fds[1], "1", 1);
    }

    void Server::replication_exec(std::shared_ptr<out_packet_i_ctx> ctx) {
        // Utils::cluck(2, "Replication exec for task %lu\n", ctx->rid);

        if(*(ctx->acceptances) == *(ctx->count_replicas)) {
            auto events = std::make_shared<std::vector<std::shared_ptr<in_packet_e_ctx_event>>>(1);

            (*events.get())[0].reset(new in_packet_e_ctx_event {
                .len = ctx->data->len,
                .data = ctx->data->data,
                .id = nullptr
            });

            in_packet_e_ctx e_ctx {
                .cnt = 1,
                .event_name_len = ctx->event->id_len,
                .event_name = ctx->event->id,
                .events = events,
                .origin = std::shared_ptr<void>(ctx, (void*)ctx.get())
            };

            uint64_t task_ids[1];
            save_event(
                e_ctx,
                0,
                std::shared_ptr<Client>(),
                task_ids,
                *(ctx->event->queue)
            );

            auto& failover = ctx->event->failover;
            failover.lock();
            failover[ctx->failover_key] = task_ids[0];
            failover.unlock();

            // TODO: this should probably make 'i' packet return 'f' // TODO: why? // oh, okay
            repl_clean(
                ctx->failover_key->len,
                ctx->failover_key->data,
                *(ctx->event)
            );

            if(ctx->rpr != nullptr) {
                // Utils::cluck(2, "asd: %u", ctx->peers_cnt);
                // const muh_str_t*& peer_id, const known_event_t*& event,
                // const uint64_t& rid
                auto x_req = Skree::Actions::X::out_init(ctx->peer_id, *(ctx->event), ctx->rid);
                size_t offset = 0;
                bool written = false;

                x_req->memorize(e_ctx.origin);

                for(const auto& _peer_id : *ctx->rpr) {
                    auto known_peers_end = known_peers.lock();
                    auto it = known_peers.find(_peer_id);
                    known_peers.unlock();

                    if(it != known_peers_end) {
                        it->second.next()->push_write_queue(x_req);
                        written = true;
                    }
                }
                // Utils::cluck(1, "qwe");

                // if(!written)
                //     delete x_req;
            }

        } else {
            // TODO: think about repl_clean()
            repl_clean(
                ctx->failover_key->len,
                ctx->failover_key->data,
                *(ctx->event)
            );

            ctx->event->unfailover(ctx->failover_key);

            // free(ctx->data->data);
            // delete ctx->data;
        }

        // Utils::cluck(1, "woot");

        // free(ctx->mutex);
        // free(ctx->acceptances);
        // free(ctx->pending);
        // free(ctx->count_replicas);
        // free(ctx);
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

        // TODO: this could possibly flap too
        const auto& flag = kv.get((char*)&id_net, sizeof(id_net));

        if(flag.size() < 1) {
            return SKREE_META_EVENTSTATE_PROCESSED;

        } else if((flag.size() == 1) && (flag[0] == '0')) {
            return SKREE_META_EVENTSTATE_PENDING;

        } else {
            return SKREE_META_EVENTSTATE_LOST;
        }
    }
}
