#pragma once

namespace Skree {
    namespace Base {
        namespace PendingWrite {
            struct QueueItem;
        }
    }
}

#include <stdlib.h>
#include <stdexcept>
#include <string.h>
#include <string>
#include <atomic>

// #include "../server.hpp"
// #include "../client.hpp"
#include "pending_read.hpp"
#include "../utils/misc.hpp"

namespace Skree {
    namespace Base {
        namespace PendingWrite {
            class QueueItem {
            private:
                char* data;
                uint32_t pos;
                uint32_t data_offset;
                uint32_t real_len;
                bool done;
                bool raw;
                const Skree::Base::PendingRead::QueueItem* cb;
                QueueItem* prev;
                std::string backtrace; // TODO: capturing backtrace in production could be slow

                QueueItem(const QueueItem& prev);
                uint32_t calc_body_len() const;
                void write(Skree::Client& client, int fd, uint32_t total_len);

                void Throw(const char* text) const {
                    std::string out;

                    out.append(text);
                    out.append(", created at\n");
                    out.append(backtrace);

                    throw std::logic_error (out.c_str());
                }

            public:
                QueueItem(uint32_t _len, char opcode)
                    : prev(nullptr)
                    , pos(0)
                    , cb(nullptr)
                    , done(false)
                    , backtrace(Utils::longmess())
                    , real_len(0)
                    , raw(false)
                {
                    data = (char*)malloc(1 + sizeof(_len) + _len);
                    real_len = 1 + sizeof(_len) + _len;
                    data_offset = 1 + sizeof(_len);
                    data[0] = opcode;

                    // Utils::cluck(3, "ctor: 0x%llx, data: 0x%llx, len: 0x%llx", (uintptr_t)this, data, len);
                }

                QueueItem(const QueueItem* _prev, uint32_t _len)
                    : QueueItem(_len, '\0')
                {
                    prev = new QueueItem(*_prev);
                }

                QueueItem(uint32_t _len, const char* _data, QueueItem* prev)
                    : prev(prev)
                    , pos(0)
                    , cb(nullptr)
                    , done(false)
                    , backtrace(Utils::longmess())
                    , real_len(_len)
                    , data((char*)_data) // Yeah
                    , data_offset(0)
                    , raw(true)
                {
                    if(prev == nullptr)
                        Throw("Raw write queue item should have a header");
                }

                ~QueueItem() {
                    // TODO
                    // if(!raw)
                    //     free(data);
                }

                void grow(uint32_t _len) {
                    if(done)
                        Throw("grow() called on read-only write queue item");

                    if(raw)
                        Throw("grow() called on raw write queue item");

                    data = (char*)realloc(data, _len + real_len);
                    real_len += _len;
                }

                void push(uint32_t _len, const void* _data) {
                    if(done)
                        Throw("push() called on read-only write queue item");

                    if(raw)
                        Throw("push() called on raw write queue item");

                    if(data_offset > real_len)
                        Throw("data_offset > real_len");

                    uint32_t rest = (real_len - data_offset);

                    if(rest < _len)
                        grow(_len - rest);

                    // Utils::cluck(6, "this: 0x%lx, *len: %u, rest: %u, rest(computed): %u, data_offset: %u, _len: %u", (uintptr_t)this, *len, rest, ((*len) - data_offset), data_offset, _len);

                    memcpy(data + data_offset, _data, _len); // TODO: should NOT copy event data
                    data_offset += _len;
                }

                void set_cb(const Skree::Base::PendingRead::QueueItem* _cb) {
                    if(done)
                        Throw("set_cb() called on read-only write queue item");

                    cb = _cb;
                }

                void finish() {
                    if(done)
                        return;
                        // Throw("finish() called on read-only write queue item");

                    done = true;
                }

                void write(Skree::Client& client, int fd) {
                    if(!done)
                        Throw("write() called on read-write write queue item");

                    write(client, fd, calc_body_len());
                }

                char get_opcode() const {
                    if(raw)
                        Throw("get_opcode() called on raw write queue item");

                    return data[0];
                }

                bool can_be_written() const {
                    if(!done)
                        Throw("can_be_written() called on read-write write queue item");

                    return ((real_len > 0) && (pos < real_len));
                }

                decltype(cb) get_cb() {
                    return cb;
                }
            };
        }
    }
}
