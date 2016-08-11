#include "pending_write.hpp"
#include "../client.hpp"
#include "../meta.hpp"

#include <unistd.h>
#include <errno.h>

namespace Skree {
    namespace Base {
        namespace PendingWrite {
            QueueItem::QueueItem(const QueueItem& _prev)
                : data(_prev.data) // TODO: this is NOT thread-safe
                , pos(0)
                , data_offset(_prev.data_offset)
                , done(true)
                , cb(nullptr)
                , prev(nullptr)
                , backtrace(_prev.backtrace)
                , real_len(_prev.real_len)
                , raw(_prev.raw)
            {
                if(!_prev.done)
                    Throw("Attempt to copy read-write write queue item");

                if(_prev.cb != nullptr)
                    Throw("Attempt to copy write queue item with callback");

                if(_prev.prev != nullptr)
                    prev = new QueueItem(*_prev.prev); // TODO: this is shit
            }

            uint32_t QueueItem::calc_body_len() const {
                if(!done)
                    Throw("calc_body_len() called on read-write write queue item");

                if(prev == nullptr)
                    return (real_len - sizeof(real_len) - 1);

                return (prev->calc_body_len() + real_len - (raw ? 0 : (sizeof(real_len) + 1)));
            }

            void QueueItem::write(Skree::Client& client, int fd, uint32_t total_len) {
                if(pos == 0) {
                    if(prev == nullptr) {
                        // original length is overwritten here
                        // TODO: this is NOT thread-safe
                        *(uint32_t*)(data + 1) = htonl(total_len);

                    } else if(!raw) {
                        pos += 1 + sizeof(real_len);
                    }
                }

                if((prev != nullptr) && prev->can_be_written()) {
                    prev->write(client, fd, total_len);
                    return;
                }
#ifdef SKREE_NET_DEBUG
                Utils::cluck(6, "about to write %u bytes, real_len: %u, pos: %u, data: 0x%llx, fd: %d", (real_len - pos), real_len, pos, (uintptr_t)data, fd);
#endif

                int written = ::write(fd, (data + pos), (real_len - pos));

#ifdef SKREE_NET_DEBUG
                Utils::cluck(5, "written %d bytes to %s/%s, len: %lu, fd: %d", written, client.get_peer_id(), client.get_conn_id(), real_len, fd);
#endif
                if(written < 0) {
                    if((errno != EAGAIN) && (errno != EINTR)) {
                        perror("write");
                        client.drop(); // TODO?
                        // Throw("woot");
                        return;
                    }

                } else {
#ifdef SKREE_NET_DEBUG
                    for(int i = 0; i < written; ++i)
                        if((i == 0) && (prev == nullptr))
                            fprintf(stderr, "written to %s/%s [%d]: %c (opcode; 0x%.2X)\n", client.get_peer_id(),client.get_conn_id(),i,data[0], (unsigned char)data[0]);
                        else
                            fprintf(stderr, "written to %s/%s [%d]: 0x%.2X\n", client.get_peer_id(),client.get_conn_id(),i,((unsigned char*)(data + pos))[i]);
#endif
                    pos += written;

                    if((pos >= real_len) && (cb != nullptr)) {
                        client.push_pending_reads_queue(cb);
                        cb = nullptr;
                    }
                }
            }
        }
    }
}
