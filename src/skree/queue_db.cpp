#include "queue_db.hpp"
// #include <execinfo.h>
#include <stdio.h>
#include <utility>

namespace Skree {
    QueueDb::QueueDb(const char* _path, size_t _file_size) : path(_path), file_size(_file_size) {
        path_len = strlen(path);

        std::string db_file_name (path, path_len);
        db_file_name.append("/skree.kch");

        kv = new DbWrapper (std::move(db_file_name));
        Reader = kv->NewReader();
    }

    QueueDb::~QueueDb() {
        delete kv;
    }
}
