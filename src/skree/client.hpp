#pragma once
namespace Skree {
    class Client;
}

#include "base/pending_read.hpp"
#include "base/pending_write.hpp"
#include "base/action.hpp"
#include "utils/misc.hpp"
#include "utils/muhev.hpp"
#include "actions/e.hpp"
#include "actions/r.hpp"
#include "server.hpp"
// #include "pending_reads/ordinary_packet.hpp"
#include "actions/w.hpp"
#include "actions/l.hpp"
#include "actions/c.hpp"
#include "actions/i.hpp"
#include "actions/x.hpp"
#include "actions/h.hpp"
#include "actions/n.hpp"

#include <fcntl.h>
#include <deque>
#include <memory>
#include <atomic>

namespace Skree {
    class Client {
    private:
        int fh;
        // struct ev_loop* loop;
        Utils::TSpinLock write_queue_mutex;
        std::shared_ptr<Utils::muh_str_t> peer_name;
        uint16_t peer_port;
        std::shared_ptr<Utils::muh_str_t> conn_name;
        uint16_t conn_port;
        std::shared_ptr<Utils::muh_str_t> peer_id;
        std::shared_ptr<Utils::muh_str_t> conn_id;
        std::deque<std::shared_ptr<Base::PendingWrite::QueueItem>> write_queue;
        std::deque<std::shared_ptr<const Base::PendingRead::QueueItem>> pending_reads;
        std::shared_ptr<sockaddr_in> s_in;
        // socklen_t s_in_len;
        Server& server;
        uint32_t protocol_version;
        uint32_t max_parallel_connections = 1;
        std::atomic<bool> destroyed;

        std::shared_ptr<Base::Action> handlers [256];// = {nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr};

        template<typename T>
        void add_action_handler();
        bool read_cb(std::shared_ptr<Base::PendingRead::Callback::Args> message);

        void ordinary_packet_cb(std::shared_ptr<Base::PendingRead::Callback::Args> message);
        std::shared_ptr<Skree::Base::PendingWrite::QueueItem> get_pending_write();
        void destroy();

        bool ShouldWrite_ = false;
        std::weak_ptr<Client> SelfWeakPtr;
        int WakeupFd;
        uint64_t ThreadId_;

        void Wakeup() const {
            if(ThreadId_ != Utils::ThreadId()) {
                ::write(WakeupFd, "1", 1);
            }
        }

    public:
        std::shared_ptr<Base::PendingRead::Callback::Args> active_read;
        static void client_cb(const NMuhEv::TEvSpec& event);

        void SetWeakPtr(const std::shared_ptr<Client>& ptr) {
            if(!SelfWeakPtr.expired()) {
                Utils::cluck(1, "SelfWeakPtr is already set");
                abort();
            }

            if(ptr.get() != this) {
                Utils::cluck(1, "Tried to set SelfWeakPtr to another client");
                abort();
            }

            SelfWeakPtr = ptr;
        }

        std::shared_ptr<Client> GetNewSharedPtr() const {
            // if(SelfWeakPtr.expired()) {
            //     Utils::cluck(1, "SelfWeakPtr is not set");
            //     abort();
            // }

            return std::shared_ptr<Client>(SelfWeakPtr); // throws on empty SelfWeakPtr
        }

        bool ShouldWrite() const {
            return ShouldWrite_;
        }

        Client(
            int _fh,
            int wakeupFd,
            std::shared_ptr<sockaddr_in> _s_in,
            // socklen_t _s_in_len,
            Server& _server
        );

        virtual ~Client();

        std::shared_ptr<Utils::muh_str_t> get_peer_name() const { return peer_name; }
        uint16_t get_peer_port() const { return peer_port; }

        void set_peer_name(const std::shared_ptr<Utils::muh_str_t>& _peer_name) {
            peer_name = _peer_name;
        }

        void set_peer_port(uint16_t _peer_port) {
            peer_port = _peer_port;
        }

        void set_peer_id(const std::shared_ptr<Utils::muh_str_t>& _peer_id) {
            peer_id = _peer_id;
        }

        uint16_t get_conn_port() {
            if(conn_port > 0) return conn_port;
            conn_port = Utils::get_port_from_sockaddr_in(s_in);
            return conn_port;
        }

        std::shared_ptr<Utils::muh_str_t> get_conn_name() {
            if(conn_name) return conn_name;
            conn_name = Utils::get_host_from_sockaddr_in(s_in);
            return conn_name;
        }

        std::shared_ptr<Utils::muh_str_t> get_peer_id() {
            if(peer_id) return peer_id;
            if(!peer_name) return std::shared_ptr<Utils::muh_str_t>();
            peer_id = Utils::make_peer_id(peer_name->len, peer_name->data, peer_port);
            return peer_id;
        }

        std::shared_ptr<Utils::muh_str_t> get_conn_id() {
            if(conn_id != nullptr) return conn_id;
            const auto& _conn_name = get_conn_name();
            conn_id = Utils::make_peer_id(_conn_name->len, _conn_name->data, get_conn_port());
            return conn_id;
        }

        void push_write_queue(
            std::shared_ptr<Skree::Base::PendingWrite::QueueItem> item,
            bool front = false
        );

        void push_pending_reads_queue(
            std::shared_ptr<const Skree::Base::PendingRead::QueueItem> item,
            bool front = false
        );

        void set_protocol_version(uint32_t _protocol_version) {
            if(_protocol_version == 0) {
                Utils::cluck(3,
                    "Client %s sent invalid protocol version: %u\n",
                    get_conn_id()->data,
                    _protocol_version
                );
            }

            protocol_version = _protocol_version;
        }

        inline const uint32_t get_protocol_version() const {
            return protocol_version;
        }

        inline const uint32_t get_max_parallel_connections() const {
            return max_parallel_connections;
        }

        inline void set_max_parallel_connections(const uint32_t& _max_parallel_connections) {
            max_parallel_connections = _max_parallel_connections;
        }

        void drop();

        inline bool is_alive() const {
            return !destroyed.load();
        }

        inline int GetFH() const {
            return fh;
        }
    };

    template<>
    inline Client* Utils::RoundRobinVector<Client*>::next() {
        auto rv = next_impl();
        int i = 0;

        while(!rv->is_alive()) {
            rv = next_impl();

            if(++i > 100)
                throw std::logic_error ("next() called on empty round-robin vector");
        }

        return rv;
    }
}
