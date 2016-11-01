#include "pending_write.hpp"
#include "../client.hpp"
#include "../meta.hpp"

#include <unistd.h>
#include <errno.h>

namespace Skree {
    namespace Base {
        namespace PendingWrite {
            QueueItem::QueueItem(const QueueItem& _prev)
                : data_last(_prev.data_last) // TODO: this is NOT thread-safe
                , length_ptr(_prev.length_ptr) // TODO: this is NOT thread-safe
                , pos(0)
                , done(true)
                , prev(_prev.prev)
                , backtrace(_prev.backtrace)
                , real_len(_prev.real_len)
                , opcode('\0')
                , data_pos(0)
                , data(_prev.data)
                , orig(_prev.orig)
            {
                if(!_prev.done)
                    Throw("Attempt to copy read-write write queue item");

                if(_prev.cb)
                    Throw("Attempt to copy write queue item with callback");

                // if(_prev.prev != nullptr)
                //     prev = new QueueItem(*_prev.prev); // TODO: this is shit
            }

            uint32_t QueueItem::calc_body_len() const {
                if(!done)
                    Throw("calc_body_len() called on read-write write queue item");

                if(!prev)
                    return (real_len - 5);

                return (prev->calc_body_len() + real_len - 5);
            }

            void QueueItem::write(Skree::Client& client, int fd, uint32_t total_len) {
                if(pos == 0) {
                    if(prev) {
                        pos += 1 + sizeof(real_len);
                        data_last = 2;

                    } else {
                        // original length is overwritten here
                        // TODO: this is NOT thread-safe
                        *length_ptr = htonl(total_len);
                    }
                }

                if((prev != nullptr) && prev->can_be_written()) {
                    prev->write(client, fd, total_len);
                    return;
                }
#ifdef SKREE_NET_DEBUG
                Utils::cluck(6, "about to write %u bytes, real_len: %u, pos: %u, fd: %d", (real_len - pos), real_len, pos, fd);
#endif

                size_t next = 0;
                uint32_t len = 0;
                const char* str = data->read(data_last, data_pos, &len, &next);
                int written = ::write(fd, str, len);

#ifdef SKREE_NET_DEBUG
                {
                    const auto& peer_id = client.get_peer_id();
                    const auto& conn_id = client.get_conn_id();

                    Utils::cluck(
                        5,
                        "written %d bytes to %s/%s, len: %lu, fd: %d",
                        written,
                        (peer_id ? peer_id->data : "(null)"),
                        (conn_id ? conn_id->data : "(null)"),
                        real_len,
                        fd
                    );
                }
#endif
                if(written < 0) {
                    if((errno != EAGAIN) && (errno != EINTR)) {
                        std::string str ("write(");

                        {
                            const auto& peer_id = client.get_peer_id();

                            if(peer_id)
                                str += peer_id->data;
                            else
                                str += "(null)";
                        }

                        str += '/';

                        {
                            const auto& conn_id = client.get_conn_id();

                            if(conn_id)
                                str += conn_id->data;
                            else
                                str += "(null)";
                        }

                        str += ')';

                        perror(str.c_str());
                        client.drop(); // TODO?
                        // Throw("woot");
                        return;
                    }

                } else {
#ifdef SKREE_NET_DEBUG
                    for(int i = 0; i < written; ++i) {
                        const auto& peer_id = client.get_peer_id();
                        const auto& conn_id = client.get_conn_id();

                        if((i == 0) && !prev) {
                            fprintf(
                                stderr,
                                "written to %s/%s [%d]: %c (opcode; 0x%.2X)\n",
                                (peer_id ? peer_id->data : "(null)"),
                                (conn_id ? conn_id->data : "(null)"),
                                i,
                                str[0],
                                (unsigned char)str[0]
                            );

                        } else {
                            fprintf(
                                stderr,
                                "written to %s/%s [%d]: 0x%.2X\n",
                                (peer_id ? peer_id->data : "(null)"),
                                (conn_id ? conn_id->data : "(null)"),
                                i,
                                ((unsigned char*)str)[i]
                            );
                        }
                    }
#endif
                    if(next != data_last) {
                        data_last = next;
                        data_pos = 0;
                    }

                    pos += written;
                    data_pos += written;

                    if((pos >= real_len) && cb) {
                        client.push_pending_reads_queue(cb);
                        cb.reset();
                    }
                }
            }
        }
    }
}
