#include "pending_write.hpp"
#include "../client.hpp"
#include "../meta.hpp"

#include <unistd.h>
#include <errno.h>

namespace Skree {
    namespace Base {
        namespace PendingWrite {
            QueueItem::QueueItem(const QueueItem& prev)
                : data(prev.data)
                , len(prev.len)
                , pos(0)
                , data_offset(prev.data_offset)
                , has_opcode(prev.has_opcode)
                , done(true)
                , cb(nullptr)
                , prev(prev.prev)
                , backtrace(prev.backtrace)
                , real_len(prev.real_len)
            {
                if(!prev.done)
                    Throw("Attempt to copy read-write write queue item");

                if(prev.cb != nullptr)
                    Throw("Attempt to copy write queue item with callback");
            }

            uint32_t QueueItem::calc_body_len() const {
                if(!done)
                    Throw("calc_body_len() called on read-write write queue item");

                if(prev == nullptr)
                    return real_len - sizeof(*len) - (has_opcode ? 1 : 0);

                return prev->calc_body_len() + real_len - sizeof(*len);
            }

            void QueueItem::write(Skree::Client& client, int fd, uint32_t total_len) {
                if(pos == 0) {
                    if(prev == nullptr) {
                        *len = htonl(total_len); // original length is overwritten here

                    } else {
                        pos += sizeof(*len);
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
                        client.drop(); // TODO
                        Throw("woot");
                        return;
                    }

                } else {
#ifdef SKREE_NET_DEBUG
                    for(int i = 0; i < written; ++i)
                        if(has_opcode && (i == 0))
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
