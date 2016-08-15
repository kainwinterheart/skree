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
#include <stack>

// #include "../server.hpp"
// #include "../client.hpp"
#include "pending_read.hpp"
#include "../utils/misc.hpp"
#include "../utils/string_sequence.hpp"

namespace Skree {
    namespace Base {
        namespace PendingWrite {
            class QueueItem {
            private:
                uint32_t pos;
                uint32_t data_pos;
                uint32_t real_len;
                bool done;
                bool raw;
                const Skree::Base::PendingRead::QueueItem* cb;
                QueueItem* prev;
                std::string backtrace; // TODO: capturing backtrace in production could be slow
                Utils::StringSequence* data_first = nullptr;
                Utils::StringSequence* data_second = nullptr;
                Utils::StringSequence* data_last = nullptr;
                std::stack<char*> stash;
                char opcode;

                QueueItem(const QueueItem& prev);
                uint32_t calc_body_len() const;
                void write(Skree::Client& client, int fd, uint32_t total_len);

                inline void Throw(const char* text) const {
                    std::string out;

                    out.append(text);
                    out.append(", created at\n");
                    out.append(backtrace);

                    throw std::logic_error (out.c_str());
                }

            public:
                inline QueueItem(uint32_t _len, char _opcode)
                    : prev(nullptr)
                    , pos(0)
                    , cb(nullptr)
                    , done(false)
                    , backtrace(Utils::longmess())
                    , real_len(5)
                    , raw(false)
                    , opcode(_opcode)
                    , data_pos(0)
                {
                    char* str = (char*)malloc(1);
                    str[0] = _opcode;
                    data_first = new Utils::StringSequence (1, str);
                    stash.push(str);

                    // See QueueItem::write
                    data_last = data_second = new Utils::StringSequence (4, nullptr);

                    data_first->concat(data_last);

                    // Utils::cluck(3, "ctor: 0x%llx, data: 0x%llx, len: 0x%llx", (uintptr_t)this, data, len);
                }

                inline QueueItem(const QueueItem* _prev, uint32_t _len)
                    : QueueItem(_len, '\0')
                {
                    prev = new QueueItem(*_prev);
                }

                inline void concat(uint32_t _len, const char* _data) {
                    if(done)
                        Throw("push() called on read-only write queue item");

                    auto* node = new Utils::StringSequence (_len, _data);

                    data_last->concat(node);
                    data_last = node;

                    real_len += _len;
                }

                inline void own_concat(uint32_t _len, const char* _data) {
                    stash.push((char*)_data); // Yeah :)

                    concat(_len, _data);
                }

                inline void copy_concat(uint32_t _len, const void* _data) {
                    char* str = (char*)malloc(_len);
                    memcpy(str, _data, _len);

                    own_concat(_len, str);
                }

                ~QueueItem() {
                    // TODO
                    // if(!raw)
                    //     free(data);
                }

                inline void set_cb(const Skree::Base::PendingRead::QueueItem* _cb) {
                    if(done)
                        Throw("set_cb() called on read-only write queue item");

                    cb = _cb;
                }

                inline void finish() {
                    if(done)
                        return;
                        // Throw("finish() called on read-only write queue item");

                    done = true;
                    data_last = data_first;
                }

                inline void write(Skree::Client& client, int fd) {
                    if(!done)
                        Throw("write() called on read-write write queue item");

                    write(client, fd, calc_body_len());
                }

                inline char get_opcode() const {
                    return opcode;
                }

                inline bool can_be_written() const {
                    if(!done)
                        Throw("can_be_written() called on read-write write queue item");

                    return ((real_len > 0) && (pos < real_len));
                }

                inline decltype(cb) get_cb() {
                    return cb;
                }
            };
        }
    }
}
