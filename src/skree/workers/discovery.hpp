#pragma once

#include "../base/worker.hpp"
#include "../client.hpp"
#include "../base/pending_read.hpp"
#include "../pending_reads/discovery.hpp"

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

namespace Skree {
    namespace Workers {
        class Discovery : public Skree::Base::Worker {
        public:
            Discovery(Skree::Server& _server, const void* _args = nullptr)
                : Skree::Base::Worker(_server, _args) {}

            virtual void run() override;

        private:
            bool do_connect(
                const char* host,
                uint32_t peer_port,
                std::shared_ptr<sockaddr_in>& addr,
                socklen_t& addr_len,
                int& fh
            );

            void cb1(Skree::Client& client);

            std::shared_ptr<Skree::Base::PendingWrite::QueueItem> cb2(
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item,
                std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args
            );

            std::shared_ptr<Skree::Base::PendingWrite::QueueItem> cb5(
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item,
                std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args
            );

            std::shared_ptr<Skree::Base::PendingWrite::QueueItem> cb6(
                Skree::Client& client,
                const Skree::Base::PendingRead::QueueItem& item,
                std::shared_ptr<Skree::Base::PendingRead::Callback::Args> args
            );

            void on_new_client(Skree::Client& client);
        };
    }
}

#include "../server.hpp" // sorry
