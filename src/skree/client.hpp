#ifndef _SKREE_CLIENT_H_
#define _SKREE_CLIENT_H_

#include "actions/c.hpp"
#include "actions/e.hpp"
#include "actions/h.hpp"
#include "actions/i.hpp"
#include "actions/l.hpp"
#include "actions/r.hpp"
#include "actions/w.hpp"
#include "actions/x.hpp"
#include "server.hpp"

namespace Skree {
    class Client {
    private:
        struct client_bound_ev_io watcher;
        int fh;
        struct ev_loop* loop;
        char* read_queue;
        size_t read_queue_length;
        size_t read_queue_mapped_length;
        std::queue<WriteQueueItem*> write_queue;
        pthread_mutex_t write_queue_mutex;
        char* peer_name;
        size_t peer_name_len;
        uint32_t peer_port;
        char* conn_name;
        size_t conn_name_len;
        uint32_t conn_port;
        char* peer_id;
        char* conn_id;
        std::deque<Skree::Base::PendingRead::QueueItem> pending_reads;
        sockaddr_in* s_in;
        socklen_t s_in_len;
        Server server;

        typedef std::unordered_map<char, Base::Action*> handlers_t;
        handlers_t handlers;

        template<typename T>
        void add_action_handler();
    public:
        Client(int _fh, struct ev_loop* _loop, sockaddr_in* _s_in, socklen_t _s_in_len);
        virtual ~Client();

        char* get_peer_name() { return peer_name; }
        uint32_t get_peer_name_len() { return peer_name_len; }
        uint32_t get_peer_port() { return peer_port; }

        uint32_t get_conn_port() {
            if(conn_port > 0) return conn_port;
            conn_port = get_port_from_sockaddr_in(s_in);
            return conn_port;
        }

        char* get_conn_name() {
            if(conn_name != NULL) return conn_name;
            conn_name = get_host_from_sockaddr_in(s_in);
            conn_name_len = strlen(conn_name);
            return conn_name;
        }

        uint32_t get_conn_name_len() {
            if(conn_name == NULL) get_conn_name();
            return conn_name_len;
        }

        char* get_peer_id() {
            if(peer_id != NULL) return peer_id;
            if(peer_name == NULL) return NULL;
            peer_id = make_peer_id(peer_name_len, peer_name, peer_port);
            return peer_id;
        }

        char* get_conn_id() {
            if(conn_id != NULL) return conn_id;
            char* _conn_name = get_conn_name();
            conn_id = make_peer_id(conn_name_len, _conn_name, get_conn_port());
            return conn_id;
        }
    }
}

#endif
