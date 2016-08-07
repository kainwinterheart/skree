#pragma once
namespace Skree {
    namespace Base {
        namespace PendingRead {
            struct QueueItem;
            class Callback;
        }
    }
    class Server;
    class Client;
}

// #include "../server.hpp"
// #include "../client.hpp"
#include "pending_write.hpp"
#include "../utils/misc.hpp"
#include <sys/types.h>
#include <stdexcept>

namespace Skree {
    namespace Base {
        namespace PendingRead {
            struct QueueItem {
                Callback* cb;
                void* ctx;
            };

            class Callback {
            protected:
                Skree::Server& server;
            public:
                class Args {
                private:
                    uint32_t len;
                    uint32_t pos;
                    char* buf;

                public:
                    char* data;
                    bool stop;
                    char opcode;
                    Skree::Base::PendingWrite::QueueItem* out;

                    uint32_t get_len() {
                        return len;
                    }

                    Args()
                        : stop(false)
                        , pos(0)
                        , out(nullptr)
                        , data(nullptr)
                        , opcode('\0')
                        , len(5)
                    {
                        buf = (char*)malloc(len);
                    }

                    ~Args() {
                        free(buf);

                        if(data != nullptr)
                            free(data);
                    }

                    void begin_data() {
                        len = ntohl(*(uint32_t*)(buf + 1));
                        opcode = buf[0];
                        pos = 0;

                        if(len > 0)
                            data = (char*)malloc(len);
                    }

                    char* end() {
                        return ((data == nullptr) ? buf : data) + pos;
                    }

                    void advance(const uint32_t _len) {
                        pos += _len;
                    }

                    uint32_t rest() {
                        return (len - pos);
                    }

                    bool should_begin_data() {
                        return (data == nullptr);
                    }
                };

                Callback(Skree::Server& _server) : server(_server) {}

                virtual Skree::Base::PendingWrite::QueueItem* run(
                    Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item,
                    Args& args
                ) = 0;

                virtual void error(
                    Skree::Client& client,
                    const Skree::Base::PendingRead::QueueItem& item
                ) {}
            };
        }
    }
}
