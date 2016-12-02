#include "db_wrapper.hpp"
#include <atomic>
#include <sys/stat.h>
#include <errno.h>

namespace Skree {
    DbWrapper::DbWrapper(std::string&& dbFileName) {
        int result = mkdir(dbFileName.c_str(), 0755);

        if((result != 0) && (errno != EEXIST)) {
            perror("mkdir");
            abort();
        }

        WT_CONNECTION* db;
        result = wiredtiger_open(
            dbFileName.c_str(),
            NULL,
            "create",
            &db
        );

        if(result != 0) {
            Utils::cluck(1, "Error opening database: %s", wiredtiger_strerror(result));
            abort();
        }

        Db = std::shared_ptr<WT_CONNECTION>(db, [](WT_CONNECTION* db) {
            int result = db->close(db, nullptr);

            if(result != 0) {
                Utils::cluck(
                    2,
                    "Failed to close database: %s",
                    wiredtiger_strerror(result)
                );

                abort();
            }
        });

        create();

        dbFileName.append(".pk");

        PkFile.reset(new Utils::MappedFile(dbFileName.c_str(), sizeof(uint64_t)));
    }
}
