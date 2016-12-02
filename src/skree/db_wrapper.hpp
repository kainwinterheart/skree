#pragma once

#include "utils/misc.hpp"
#include "utils/mapped_file.hpp"
#include <wiredtiger.h>
// #include <pthread.h>
// #include <vector>
// #include <string>

namespace Skree {
    // typedef std::unordered_map<char*, Utils::muh_str_t*, Utils::char_pointer_hasher, Utils::char_pointer_comparator> get_keys_result_t;

    class DbWrapper {
    public:
        class Session {
        private:
            DbWrapper& Db;
            std::shared_ptr<WT_SESSION> Session_;
            std::shared_ptr<WT_CURSOR> OwTCursor;
            std::shared_ptr<WT_CURSOR> OwFCursor;

            static void AssertOk(const char* const str, const int result) {
                if(result != 0) {
                    Utils::cluck(
                        3,
                        "%s: %s",
                        str,
                        wiredtiger_strerror(result)
                    );

                    abort();
                }
            }

            std::shared_ptr<WT_CURSOR> NewCursor(const char* const options = nullptr) {
                WT_CURSOR* cursor;
                AssertOk("Error opening cursor", Session_->open_cursor(
                    Session_.get(),
                    "table:skree",
                    nullptr,
                    options,
                    &cursor
                ));

                cursor->key_format = "u";
                cursor->value_format = "u";

                return std::shared_ptr<WT_CURSOR>(cursor, [](WT_CURSOR* cursor) {
                    AssertOk("Failed to close cursor", cursor->close(cursor));
                });
            }

        public:
            Session(DbWrapper& db, bool create = false) : Db(db) {
                WT_SESSION* session;
                AssertOk("Error opening session", Db.Db->open_session(
                    Db.Db.get(),
                    nullptr,
                    nullptr,
                    &session
                ));

                Session_ = std::shared_ptr<WT_SESSION>(session, [](WT_SESSION* session) {
                    AssertOk("Failed to close session", session->close(session, nullptr));
                });

                if(create) {
                    AssertOk("Failed to create table", session->create(
                        session,
                        "table:skree",
                        "key_format=u,value_format=u"
                    ));
                }

                OwTCursor = NewCursor("overwrite=true,raw");
                OwFCursor = NewCursor("overwrite=false,raw");
            }

            bool add(
                const char * kbuf,
                size_t ksiz,
                const char * vbuf,
                size_t vsiz
            ) {
                // Utils::cluck(2, "db.add(%s)", kbuf);
                // Utils::cluck(3, "db.add: %lu, %lu", ksiz, vsiz);
                WT_ITEM key {
                    .data = kbuf,
                    .size = ksiz,
                };

                OwFCursor->set_key(OwFCursor.get(), &key);

                WT_ITEM value {
                    .data = vbuf,
                    .size = vsiz,
                };

                OwFCursor->set_value(OwFCursor.get(), &value);

                return (OwFCursor->insert(OwFCursor.get()) == 0);
            }

            bool set(
                const char * kbuf,
                size_t ksiz,
                const char * vbuf,
                size_t vsiz
            ) {
                WT_ITEM key {
                    .data = kbuf,
                    .size = ksiz,
                };

                OwTCursor->set_key(OwTCursor.get(), &key);

                WT_ITEM value {
                    .data = vbuf,
                    .size = vsiz,
                };

                OwTCursor->set_value(OwTCursor.get(), &value);

                return (OwTCursor->insert(OwTCursor.get()) == 0);
            }

            bool remove(
                const char * kbuf,
                size_t ksiz
            ) {
                // fprintf(stderr, "db_wrapper::remove(");
                // for(size_t i = 0; i < ksiz; ++i) {
                //     fprintf(stderr, "%.2X", kbuf[i]);
                // }
                // Utils::cluck(1, ")");
                // abort();
                WT_ITEM key {
                    .data = kbuf,
                    .size = ksiz,
                };

                OwTCursor->set_key(OwTCursor.get(), &key);

                return (OwTCursor->remove(OwTCursor.get()) == 0);
            }

            bool cas(
                const char * kbuf,
                size_t ksiz,
                const char * ovbuf,
                size_t ovsiz,
                const char * nvbuf,
                size_t nvsiz
            ) {
                begin_transaction();

                const auto& value = get(kbuf, ksiz);

                if(value.size() != ovsiz)
                    return false;

                if(strncmp(value.data(), ovbuf, ovsiz) != 0)
                    return false;

                auto result = set(kbuf, ksiz, nvbuf, nvsiz);

                if(!end_transaction(result))
                    return false;

                return result;
            }

            bool begin_transaction() {
                return (Session_->begin_transaction(Session_.get(), "snapshot") == 0);
            }

            bool end_transaction(bool commit = true) {
                if(commit) {
                    return (Session_->commit_transaction(Session_.get(), nullptr) == 0);

                } else {
                    return (Session_->rollback_transaction(Session_.get(), nullptr) == 0);
                }
            }

            std::string get(
                const char * kbuf,
                size_t ksiz
            ) {
                WT_ITEM key {
                    .data = kbuf,
                    .size = ksiz,
                };

                OwTCursor->set_key(OwTCursor.get(), &key);

                int result = OwTCursor->search(OwTCursor.get());
                std::string out;

                if(result == 0) {
                    WT_ITEM value;
                    AssertOk("Failed to fetch data", OwTCursor->get_value(OwTCursor.get(), &value));
                    out.assign((const char*)value.data, value.size);

                } else if(result != WT_NOTFOUND) {
                    AssertOk("Failed to get key", result);
                }

                return out;
            }

            int32_t check(
                const char * kbuf,
                size_t ksiz
            ) {
                return get(kbuf, ksiz).size();
            }

            void create(
                const char* const dataSource,
                const char* const options
            ) {
                AssertOk("Failed to create data source", Session_->create(
                    Session_.get(),
                    dataSource,
                    options
                ));
            }
        };

    private:
        std::shared_ptr<WT_CONNECTION> Db;
        std::shared_ptr<Utils::MappedFile> PkFile;

    public:
        DbWrapper(std::string&& dbFileName);

        std::shared_ptr<Session> NewSession(bool create = false) {
            return std::make_shared<Session>(*this, create);
        }

        void create() {
            NewSession(true);
        }

        bool add(
            const char * kbuf,
            size_t ksiz,
            const char * vbuf,
            size_t vsiz
        ) {
            return NewSession()->add(kbuf, ksiz, vbuf, vsiz);
        }

        bool set(
            const char * kbuf,
            size_t ksiz,
            const char * vbuf,
            size_t vsiz
        ) {
            return NewSession()->set(kbuf, ksiz, vbuf, vsiz);
        }

        bool remove(
            const char * kbuf,
            size_t ksiz
        ) {
            return NewSession()->remove(kbuf, ksiz);
        }

        uint64_t increment(uint64_t num) {
            static_assert(sizeof(std::atomic<uint64_t>) == sizeof(uint64_t), "sizeof(std::atomic<uint64_t>) == sizeof(uint64_t)");
            return std::atomic_fetch_add((std::atomic<uint64_t>*)PkFile->begin(), num) + num;
        }

        bool cas(
            const char * kbuf,
            size_t ksiz,
            const char * ovbuf,
            size_t ovsiz,
            const char * nvbuf,
            size_t nvsiz
        ) {
            return NewSession()->cas(kbuf, ksiz, ovbuf, ovsiz, nvbuf, nvsiz);
        }

        bool synchronize() {
            PkFile->sync();
            return true;
        }

        std::string get(
            const char * kbuf,
            size_t ksiz
        ) {
            return NewSession()->get(kbuf, ksiz);
        }

        int32_t check(
            const char * kbuf,
            size_t ksiz
        ) {
            return NewSession()->get(kbuf, ksiz).size();
        }
    };
}
