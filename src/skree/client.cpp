#include "client.hpp"
#include "meta.hpp"
// #include "base/pending_read.hpp"

namespace Skree {
    void Client::ordinary_packet_cb(std::shared_ptr<Base::PendingRead::Callback::Args> message) {
        if(handlers[message->opcode] == nullptr) {
            Utils::cluck(2, "Unknown packet header: 0x%.2X", message->opcode);

        } else {
            handlers[message->opcode]->in(message);
            ++(server.stat_num_requests_detailed[message->opcode]);
        }
    }

    bool Client::read_cb(std::shared_ptr<Base::PendingRead::Callback::Args> message) {
        if(!pending_reads.empty()) {
            // Pending reads queue is a top priority callback
            // If there is a pending read - incoming data should
            // be passed to such a callback
#ifdef SKREE_NET_DEBUG
            Utils::cluck(2, "Got opcode: 0x%.2X, pending_reads not empty", message->opcode);
#endif
            auto item = pending_reads.front();

            if(
                (message->opcode == SKREE_META_OPCODE_K)
                || (message->opcode == SKREE_META_OPCODE_F)
                || (message->opcode == SKREE_META_OPCODE_A)
            ) {
                pending_reads.pop_front();

                // ***
#ifdef SKREE_NET_DEBUG
                Utils::cluck(2, "About to run original callback with a packet of size %lu bytes", message->get_len());
#endif
                auto new_item = item->cb->run(*this, *item, message);

                if(message->stop) {
                    drop();
                    return false;

                } else if(new_item) {
                    push_write_queue(new_item);
                }
                // ***

            } else if(
                (protocol_version > 0)
                || (message->opcode == Skree::Actions::N::opcode()) // TODO: crutch?
            ) {
                // If pending read waits for opcode, and it is not
                // the reply opcode we've got here - process data as
                // ordinary inbound packet

                // Utils::cluck(1, "ordinary_packet_cb 1\n");
#ifdef SKREE_NET_DEBUG
                Utils::cluck(2, "Got opcode: 0x%.2X, pending_reads is empty, but queue item is not suitable", message->opcode);
#endif
                ordinary_packet_cb(message);

            } else {
                Utils::cluck(3, "invalid pending read, opcode: %c (0x%.2X)", message->opcode, message->opcode);
                drop();
                return false;
            }

        } else if(
            (protocol_version > 0)
            || (message->opcode == Skree::Actions::N::opcode()) // TODO: crutch?
        ) {
            // There is no pending reads, so data should be processed
            // as ordinary inbound packet

            // Utils::cluck(1, "ordinary_packet_cb 2\n");
#ifdef SKREE_NET_DEBUG
            Utils::cluck(2, "Got opcode: 0x%.2X, pending_reads is empty", message->opcode);
#endif
            ordinary_packet_cb(message);

        } else {
            Utils::cluck(1, "no pending reads and no protocol_version");
            drop();
            return false;
        }

        if(message->out != nullptr) {
            message->out->finish();
            ++(server.stat_num_responses_detailed[message->out->get_opcode()]);
            push_write_queue(message->out);
        }

        return true;
    }

    Client::Client(
        int _fh,
        int wakeupFd,
        std::shared_ptr<sockaddr_in> _s_in,
        // socklen_t _s_in_len,
        Server& _server
    )
        : fh(_fh)
        // , loop(_loop)
        , s_in(_s_in)
        // , s_in_len(_s_in_len)
        , server(_server)
        , WakeupFd(wakeupFd)
        , ThreadId_(Utils::ThreadId())
    {
        conn_port = 0;
        protocol_version = 0;
        destroyed = false;

        Utils::SetupSocket(fh, server.discovery_timeout_milliseconds); // TODO: check return value
        fcntl(fh, F_SETFL, fcntl(fh, F_GETFL, 0) | O_NONBLOCK);

        add_action_handler<Actions::W>();
        add_action_handler<Actions::L>();
        add_action_handler<Actions::E>();
        add_action_handler<Actions::R>();
        add_action_handler<Actions::C>();
        add_action_handler<Actions::I>();
        add_action_handler<Actions::X>();
        add_action_handler<Actions::H>();
        add_action_handler<Actions::N>();
    }

    template<typename T>
    void Client::add_action_handler() {
        handlers[T::opcode()].reset(new T (server, *this));
    }

    Client::~Client() {
        destroy();
    }

    void Client::destroy() {
        bool _destroyed = destroyed.exchange(true);

        if(_destroyed)
            return;

        // if((peer_id != nullptr) || (conn_id != nullptr))
            // Utils::cluck(3, "DROP(%s/%s)", peer_id, conn_id);

        auto& known_peers = server.known_peers;
        auto known_peers_end = known_peers.lock();

        shutdown(fh, SHUT_RDWR);
        close(fh);
        // free(s_in);

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

            if(_cb != nullptr)
                _cb->error(*this, *item);

            pending_reads.pop_front();
            // delete item; //TODO?
        }

        {
            Utils::TSpinLockGuard guard(write_queue_mutex);

            while(!write_queue.empty()) {
                auto item = write_queue.front();
                auto __cb = item->get_cb();

                if(__cb) {
                    auto _cb = __cb->cb;

                    if(_cb != nullptr)
                        _cb->error(*this, *__cb);
                }

                write_queue.pop_front();
                // delete item; // TODO?
            }
        }

        // if(peer_name != nullptr) free(peer_name);
        // if(conn_name != nullptr) free(conn_name);
    }

    std::shared_ptr<Skree::Base::PendingWrite::QueueItem> Client::get_pending_write() {
        Utils::TSpinLockGuard guard(write_queue_mutex);

        while(!write_queue.empty()) {
            auto item = write_queue.front();

            if(item->can_be_written()) {
                return item;
            }

            write_queue.pop_front();
            // delete item; // TODO?
        }

        // ev_io_set(&watcher.watcher, fh, EV_READ);
        ShouldWrite_ = false;

        return std::shared_ptr<Skree::Base::PendingWrite::QueueItem>();
    }

    void Client::push_write_queue(
        std::shared_ptr<Skree::Base::PendingWrite::QueueItem> item, bool front
    ) {
        item->finish();

        if(destroyed)
            return;

        Utils::TSpinLockGuard guard(write_queue_mutex);

        if(write_queue.empty()) {
            ShouldWrite_ = true;//ev_io_set(&watcher.watcher, fh, EV_READ | EV_WRITE);
            Wakeup();
        }

        if(front)
            write_queue.push_front(item);
        else
            write_queue.push_back(item);
    }

    void Client::push_pending_reads_queue(
        std::shared_ptr<const Skree::Base::PendingRead::QueueItem> item, bool front
    ) {
        // if(item->len == 0) {
        //     Utils::cluck(1, "Client::push_pending_reads_queue() got zero-length pending read, ignoring it\n");
        //     return;
        // }

        // if(item->len == 9) {
        //     throw std::logic_error ("trap");
        // }

        if(destroyed)
            return;

        if(front)
            pending_reads.push_front(item);
        else
            pending_reads.push_back(item);
    }

    void Client::client_cb(const NMuhEv::TEvSpec& event) {
        Client* client = (Client*)event.Ctx;

        if(event.Flags & NMuhEv::MUHEV_FLAG_ERROR) {
            Utils::cluck(1, "EV_ERROR");
            client->drop();
            return;
        }

        if(event.Flags & NMuhEv::MUHEV_FLAG_EOF) {
            // Utils::cluck(1, "EV_EOF");
            client->drop();
            return;
        }

        if(event.Filter & NMuhEv::MUHEV_FILTER_READ) {
            if(!client->active_read)
                client->active_read.reset(new Skree::Base::PendingRead::Callback::Args);

            auto active_read = client->active_read;

            if(active_read->rest() == 0)
                throw std::logic_error ("Zero-length active_read");

            while(true) {
                int read = recvfrom(
                    event.Ident,
                    active_read->end(),
                    active_read->rest(),
                    MSG_DONTWAIT,
                    NULL,
                    0
                );

                if(read > 0) {
    #ifdef SKREE_NET_DEBUG
                    {
                        const auto& peer_id = client->get_peer_id();
                        const auto& conn_id = client->get_conn_id();

                        for(int i = 0; i < read; ++i)
                            fprintf(
                                stderr,
                                "read from %s/%s [%d]: 0x%.2X\n",
                                (peer_id ? peer_id->data : "(null)"),
                                (conn_id ? conn_id->data : "(null)"),
                                i,
                                ((unsigned char*)(active_read->end()))[i]
                            );
                    }
    #endif
                    active_read->advance(read);
    #ifdef SKREE_NET_DEBUG
                    Utils::cluck(2, "[client_cb::1] rest=%u", active_read->rest());
    #endif
                    if(active_read->rest() == 0) {
                        if(active_read->should_begin_data()) {
                            active_read->begin_data();
    #ifdef SKREE_NET_DEBUG
                            Utils::cluck(2, "[client_cb::2] rest=%u", active_read->rest());
    #endif
                            if(active_read->rest() == 0) {
    #ifdef SKREE_NET_DEBUG
                                Utils::cluck(1, "[client_cb] data is empty, run");
    #endif
                                if(client->read_cb(active_read))
                                    client->active_read.reset();

                                break;
                            }

                        } else {
    #ifdef SKREE_NET_DEBUG
                            Utils::cluck(1, "[client_cb] data already began, run");
    #endif
                            if(client->read_cb(active_read))
                                client->active_read.reset();

                            break;
                        }
                    }

                } else if(read < 0) {
                    if((errno != EAGAIN) && (errno != EINTR)) {
                        std::string str ("recv(");

                        {
                            const auto& peer_id = client->get_peer_id();

                            if(peer_id)
                                str += peer_id->data;
                            else
                                str += "(null)";
                        }

                        str += '/';

                        {
                            const auto& conn_id = client->get_conn_id();

                            if(conn_id == nullptr)
                                str += conn_id->data;
                            else
                                str += "(null)";
                        }

                        str += ')';

                        perror(str.c_str());
                        client->drop();
                        return;

                    } else if(errno == EAGAIN) {
                        break;
                    }

                } else {
                    // if(client->get_peer_id() != nullptr)
                    //     Utils::cluck(3, "%s/%s disconnected", client->get_peer_id(), client->get_conn_id());

                    client->drop();
                    return;
                }
            }
        }

        if(event.Filter & NMuhEv::MUHEV_FILTER_WRITE) {
            while(true) {
                auto item = client->get_pending_write();

                if(
                    (item != nullptr)
                    && item->write(*client, event.Ident)
                ) {
                    // ok

                } else {
                    break;
                }
            }
        }
    }

    void Client::drop() {
        destroy();
        // delete this; // TODO: does this break ReplicationPingTask somehow?
    }
}
