#pragma once
namespace Skree {
    class Client;
}

#include "base/pending_read.hpp"
#include "base/pending_write.hpp"
#include "base/action.hpp"
#include "utils/misc.hpp"
#include "actions/e.hpp"
#include "actions/r.hpp"
#include "server.hpp"
#include "pending_reads/noop.hpp"
#include "pending_reads/ordinary_packet.hpp"
#include <fcntl.h>
#include "actions/w.hpp"
#include "actions/l.hpp"
#include "actions/c.hpp"
#include "actions/i.hpp"
#include "actions/x.hpp"
#include "actions/h.hpp"

#include <deque>
#include <memory>

namespace Skree {
    class Client {
    private:
        struct Utils::client_bound_ev_io watcher;
        int fh;
        struct ev_loop* loop;
        char* read_queue;
        size_t read_queue_length;
        size_t read_queue_mapped_length;
        pthread_mutex_t write_queue_mutex;
        char* peer_name;
        size_t peer_name_len;
        uint16_t peer_port;
        char* conn_name;
        size_t conn_name_len;
        uint16_t conn_port;
        char* peer_id;
        char* conn_id;
        std::deque<Base::PendingWrite::QueueItem*> write_queue;
        std::deque<const Base::PendingRead::QueueItem*> pending_reads;
        sockaddr_in* s_in;
        socklen_t s_in_len;
        Server& server;
        uint32_t protocol_version;

        typedef Base::Action* handlers_t;
        handlers_t handlers [256] = {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr};

        template<typename T>
        void add_action_handler();
        void read_cb();

        static void client_cb(struct ev_loop* loop, ev_io* _watcher, int events);

        void push_read_queue(size_t len, char* data);

        void ordinary_packet_cb(
            const char& opcode, char*& out_data,
            size_t& out_len, const size_t& in_packet_len
        );

        Skree::Base::PendingWrite::QueueItem* get_pending_write();
    public:
        Client(int _fh, struct ev_loop* _loop, sockaddr_in* _s_in, socklen_t _s_in_len, Server& _server);
        virtual ~Client();

        const char* get_peer_name() const { return peer_name; }
        const uint32_t get_peer_name_len() const { return peer_name_len; }
        const uint16_t get_peer_port() const { return peer_port; }

        void set_peer_name(uint32_t _peer_name_len, char* _peer_name) {
            peer_name_len = _peer_name_len;
            peer_name = _peer_name;
        }

        void set_peer_port(uint16_t _peer_port) {
            peer_port = _peer_port;
        }

        void set_peer_id(char* _peer_id) {
            peer_id = _peer_id;
        }

        uint16_t get_conn_port() {
            if(conn_port > 0) return conn_port;
            conn_port = Utils::get_port_from_sockaddr_in(s_in);
            return conn_port;
        }

        char* get_conn_name() {
            if(conn_name != nullptr) return conn_name;
            conn_name = Utils::get_host_from_sockaddr_in(s_in);
            conn_name_len = strlen(conn_name);
            return conn_name;
        }

        uint32_t get_conn_name_len() {
            if(conn_name == nullptr) get_conn_name();
            return conn_name_len;
        }

        char* get_peer_id() {
            if(peer_id != nullptr) return peer_id;
            if(peer_name == nullptr) return nullptr;
            peer_id = Utils::make_peer_id(peer_name_len, peer_name, peer_port);
            return peer_id;
        }

        char* get_conn_id() {
            if(conn_id != nullptr) return conn_id;
            char* _conn_name = get_conn_name();
            conn_id = Utils::make_peer_id(conn_name_len, _conn_name, get_conn_port());
            return conn_id;
        }

        void push_write_queue(
            Skree::Base::PendingWrite::QueueItem* item,
            bool front = false
        );

        void push_pending_reads_queue(
            const Skree::Base::PendingRead::QueueItem* item,
            bool front = false
        );

        void set_protocol_version(uint32_t _protocol_version) {
            if(_protocol_version == 0) {
                fprintf(
                    stderr,
                    "Client %s sent invalid protocol version: %u\n",
                    get_conn_id(),
                    _protocol_version
                );
            }

            protocol_version = _protocol_version;
        }

        uint32_t get_protocol_version() {
            return protocol_version;
        }
    };
}
