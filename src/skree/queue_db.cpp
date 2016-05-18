#include "queue_db.hpp"

namespace Skree {
    void QueueDb::get_page_num(
        const char* name, int& fh,
        uint64_t& page_num, uint64_t& offset
    ) const {
        if(fh == -1) {
            auto name_len = strlen(name);
            char file [path_len + 1 + name_len + 1];

            memcpy(file, path, path_len);
            file[path_len] = '/';
            memcpy(file + path_len + 1, name, name_len);
            file[path_len + 1 + name_len] = '\0';

            fh = open(file, O_RDWR | O_CREAT);
        }

        if(fh == -1) {
            perror("open");
            exit(1);
        }

        read_uint64(fh, page_num);
        read_uint64(fh, offset);
    }

    void QueueDb::read_uint64(int& fh, uint64_t& dest) const {
        int read_total = 0;
        int read;

        while(
            ((read = ::read(fh, (&dest + read_total), (8 - read_total))) > 0)
            && (read_total < 8)
        ) {
            read_total += read;
        }

        dest = ((read_total == 8) ? ntohll(dest) : 0);
    }

    void QueueDb::open_page(
        int& fh, int flag, const uint64_t& num, char*& addr
    ) const {
        if(fh != -1) return;

        char file [path_len + 1 + 21 + 1];
        int access_flag = 0;
        int mmap_prot = 0;
        bool known_flag = false;

        memcpy(file, path, path_len);
        file[path_len] = '/';
        sprintf(file + path_len + 1, "%llu", num);

        if((flag & O_RDONLY) || (flag == O_RDONLY) || (flag & O_RDWR) || (flag == O_RDWR)) {
            access_flag |= R_OK;
            mmap_prot |= PROT_READ;
            known_flag = true;
        }

        if((flag & O_WRONLY) || (flag == O_WRONLY) || (flag & O_RDWR) || (flag == O_RDWR)) {
            access_flag |= W_OK;
            mmap_prot |= PROT_WRITE;
            known_flag = true;
        }

        if(!known_flag) {
            // fprintf(stderr, "flag: %lld, access_flag: %lld, mmap_prot: %lld, O_RDONLY: %lld, O_WRONLY: %lld, O_RDWR: %lld\n", flag, access_flag, mmap_prot, O_RDONLY, O_WRONLY, O_RDWR);
            throw new std::logic_error ("QueueDb::open_page: Bad flags");
        }

        if(access(file, access_flag) == -1) {
            int fh = open(file, O_RDWR | O_CREAT);

            if(fh == -1) {
                perror("open");
                exit(1);
            }

            size_t total = 0;
            char batch [SKREE_QUEUEDB_ZERO_BATCH_SIZE];
            memset(batch, 0, SKREE_QUEUEDB_ZERO_BATCH_SIZE);
            size_t written;

            while(total < file_size) {
                written = ::write(fh, batch, (
                    ((file_size - total) > SKREE_QUEUEDB_ZERO_BATCH_SIZE)
                        ? SKREE_QUEUEDB_ZERO_BATCH_SIZE
                        : (file_size - total)
                ));

                total += written;
            }

            close(fh);
        }

        fh = open(file, flag);

        if(fh == -1) {
            perror("open");
            exit(1);
        }

        addr = (char*)mmap(0, file_size, mmap_prot, MAP_FILE | MAP_SHARED | MAP_NOCACHE, fh, 0);

        if(addr == MAP_FAILED) {
            perror("mmap");
            exit(1);
        }
    }

    QueueDb* QueueDb::get_next_page() {
        if(next_page != NULL) return next_page;

        next_page = new QueueDb(
            path, file_size, read_page_num + 1,
            write_page_num + 1, read_page_num_fh,
            write_page_num_fh, kv
        );

        return next_page;
    }

    void QueueDb::sync_read_offset(bool commit) {
        if(read_rollbacks.empty()) {
            throw new std::logic_error ("There are no read rollbacks, what are you syncing?");
        }

        if(commit) {
            lseek(read_page_num_fh, 0, SEEK_SET);

            uint64_t _read_page_num = read_page_num;
            uint64_t _read_page_offset = read_page_offset;
            auto db = this;

            while(_read_page_offset == file_size) {
                db = db->next_page;
                _read_page_num = db->read_page_num;
                _read_page_offset = db->read_page_offset;
            }

            _read_page_num = htonll(_read_page_num);
            _read_page_offset = htonll(_read_page_offset);

            ::write(read_page_num_fh, &_read_page_num, sizeof(_read_page_num));
            ::write(read_page_num_fh, &_read_page_offset, sizeof(_read_page_offset));
            fsync(read_page_num_fh);

            close_if_can();

            while(!read_rollbacks.empty()) {
                auto _rollback = read_rollbacks.top();
                read_rollbacks.pop();

                _free_read_rollback(_rollback);
            }

        } else {
            while(!read_rollbacks.empty()) {
                auto _rollback = read_rollbacks.top();
                read_rollbacks.pop();

                _rollback_read(_rollback);
            }
        }
    }

    void QueueDb::sync_write_offset() {
        lseek(write_page_num_fh, 0, SEEK_SET);

        uint64_t _write_page_num = write_page_num;
        uint64_t _write_page_offset = write_page_offset;
        auto db = this;

        while(_write_page_offset == file_size) {
            db = db->next_page;
            _write_page_num = db->write_page_num;
            _write_page_offset = db->write_page_offset;
        }

        _write_page_num = htonll(_write_page_num);
        _write_page_offset = htonll(_write_page_offset);

        ::write(write_page_num_fh, &_write_page_num, sizeof(_write_page_num));
        ::write(write_page_num_fh, &_write_page_offset, sizeof(_write_page_offset));
        fsync(write_page_num_fh);

        close_if_can();
    }

    void QueueDb::close_if_can() {
        if(next_page == NULL) return;

        next_page->close_if_can();

        if(
            (read_page_offset == file_size)
            && (read_page != next_page->read_page)
        ) {
            munmap(read_page, file_size);
            close(read_page_fh);

            read_page = next_page->read_page;
            read_page_fh = next_page->read_page_fh;
        }

        if(
            (write_page_offset == file_size)
            && (write_page != next_page->write_page)
        ) {
            munmap(write_page, file_size);
            close(write_page_fh);

            write_page = next_page->write_page;
            write_page_fh = next_page->write_page_fh;
        }

        while(
            (next_page != NULL)
            && (read_page == next_page->read_page)
            && (write_page == next_page->write_page)
        ) {
            read_page_num = next_page->read_page_num;
            write_page_num = next_page->write_page_num;

            read_page_offset = next_page->read_page_offset;
            write_page_offset = next_page->write_page_offset;

            auto prev_next_page = next_page;
            next_page = next_page->next_page;

            delete prev_next_page;
        }
    }

    read_rollback_t* QueueDb::_read(uint64_t len, unsigned char* dest) {
        uint64_t rest = (file_size - read_page_offset);
        auto rollback = new read_rollback_t {
            .l1 = 0,
            .l2 = NULL
        };

        if(rest >= len) {
            memcpy(dest, read_page + read_page_offset, len);
            read_page_offset += len;
            rollback->l1 = len;

            for(int i = 0; i < len; ++i)
                printf("read byte [%d]: 0x%.2X\n", i,dest[i]);

        } else {
            if(rest > 0) {
                len -= rest;
                memcpy(dest, read_page + read_page_offset, rest);
                read_page_offset += rest;
                rollback->l1 = rest;
            }

            auto next = get_next_page();
            rollback->l2 = next->_read(len, dest + rest);
        }

        return rollback;
    }

    void QueueDb::_rollback_read(read_rollback_t* rollback) {
        if(rollback->l1 > 0) {
            read_page_offset -= rollback->l1;
        }

        if(rollback->l2 != NULL) {
            auto next = get_next_page();
            next->_rollback_read(rollback->l2);
        }

        delete rollback;
    }

    void QueueDb::_free_read_rollback(read_rollback_t* rollback) const {
        if(rollback->l2 != NULL) {
            _free_read_rollback(rollback->l2);
        }

        delete rollback;
    }

    void QueueDb::_write(uint64_t len, const unsigned char* src) {
        uint64_t rest = (file_size - write_page_offset);

        if(rest >= len) {
            // printf("write_page: 0x%lx, write_page_offset: %llu, src: 0x%lx\n", (intptr_t)write_page, write_page_offset, (intptr_t)src);
            memcpy(write_page + write_page_offset, src, len);
            write_page_offset += len;

        } else {
            if(rest > 0) {
                len -= rest;
                memcpy(write_page + write_page_offset, src, rest);
                write_page_offset += rest;
            }

            auto next = get_next_page();
            next->_write(len, src + rest);
        }
    }

    char* QueueDb::read() {
        if(!read_rollbacks.empty()) {
            throw new std::logic_error ("You haven't committed previous read, why are you trying to read more?");
        }

        pthread_mutex_lock(&write_page_mutex);

        if(
            (read_page_num == write_page_num)
            && (read_page_offset >= write_page_offset)
        ) {
            pthread_mutex_unlock(&write_page_mutex);
            // TODO: read rollbacks may not be needed after this
            return NULL;
        }

        pthread_mutex_unlock(&write_page_mutex);

        char* out = NULL;
        uint64_t len;

        // pthread_mutex_lock(&read_page_mutex);

        auto rollback1 = _read(sizeof(len), (unsigned char*)&len);
        len = ntohll(len);

        if(len == 0) {
            // fprintf(stderr, "QueueDb: got zero-length item, rollback\n");
            _rollback_read(rollback1);
            throw new std::logic_error ("QueueDb: got zero-length item, rollback");

        } else {
            out = (char*)malloc(len);
            auto rollback2 = _read(len, (unsigned char*)out);
            // _free_read_rollback(rollback2);
            // _free_read_rollback(rollback1);
            read_rollbacks.push(rollback1);
            read_rollbacks.push(rollback2);
        }

        // pthread_mutex_unlock(&read_page_mutex);

        return out;
    }

    QueueDb::WriteStream* QueueDb::write() {
        return new QueueDb::WriteStream (*this);
    }

    void QueueDb::WriteStream::write(uint64_t len, void* data) {
        db._write(len, (const unsigned char*)data);
        total_len += len;
    }

    QueueDb::WriteStream::WriteStream(QueueDb& _db) : db(_db) {
        pthread_mutex_lock(&(db.write_page_mutex));

        last_page = &db;

        while(last_page->file_size == last_page->write_page_offset) {
            last_page = last_page->get_next_page();
        }

        begin_offset = last_page->write_page_offset;
        total_len = 0;

        const uint64_t _total_len (htonll(total_len));
        db._write(sizeof(_total_len), (const unsigned char*)&_total_len);
    }

    QueueDb::WriteStream::~WriteStream() {
        uint64_t end_offset = last_page->write_page_offset;

        last_page->write_page_offset = begin_offset;

        bool wrap = ((last_page->file_size - last_page->write_page_offset) < sizeof(total_len));
        uint64_t next_end_offset;

        if(wrap) {
            auto& next = last_page->next_page;
            next_end_offset = next->write_page_offset;
            next->write_page_offset = 0;
        }

        if(total_len > 0) {
            const uint64_t _total_len (htonll(total_len));
            db._write(sizeof(_total_len), (const unsigned char*)&_total_len);

            last_page->write_page_offset = end_offset;

            if(wrap) {
                auto& next = last_page->next_page;
                next->write_page_offset = next_end_offset;
            }

            db.sync_write_offset();
            pthread_mutex_unlock(&(db.write_page_mutex));

        } else {
            pthread_mutex_unlock(&(db.write_page_mutex));
            throw new std::logic_error("QueueDb: zero-length write, ignoring it");
        }
    }
}
