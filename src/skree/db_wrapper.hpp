#pragma once

#include "utils/misc.hpp"
#include "utils/mapped_file.hpp"
#include <wiredtiger.h>
#include <utility>
// #include <pthread.h>
// #include <vector>
// #include <string>

namespace Skree {
    // typedef std::unordered_map<char*, Utils::muh_str_t*, Utils::char_pointer_hasher, Utils::char_pointer_comparator> get_keys_result_t;

    class DbWrapper {
    public:
        class TSession {
        public:
            enum ESessionTable {
                ST_KV = 1,
                ST_QUEUE = 2,
            };

        private:
            DbWrapper& Db;
            std::shared_ptr<WT_SESSION> Session_;
            std::shared_ptr<WT_CURSOR> OwTCursor;
            std::shared_ptr<WT_CURSOR> OwFCursor;
            ESessionTable Table;

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

            const char* TableName() {
                return ((Table == ST_KV) ? "table:kv" : "table:queue");
            }

            const char* TableFormat() {
                return ((Table == ST_KV)
                    ? "key_format=u,value_format=u"
                    : "key_format=r,value_format=u");
            }

            template<typename TReturn, typename TCallback, typename... TArgs>
            TReturn WithPackedKey(const uint64_t key, TCallback&& cb, TArgs&&... args) {
                if(Table != ST_QUEUE) {
                    Utils::cluck(1, "WithPackedKey() is only allowed for ST_QUEUE");
                    abort();
                }

                size_t ksiz;
                AssertOk("Failed to measure the size of the key", wiredtiger_struct_size(
                    Session_.get(),
                    &ksiz,
                    "q",
                    key
                ));

                char* kbuf = (char*)malloc(ksiz);
                std::shared_ptr<char> _kbuf (kbuf, [](char* kbuf) {
                    free(kbuf);
                });

                AssertOk("Failed to pack the key", wiredtiger_struct_pack(
                    Session_.get(),
                    kbuf,
                    ksiz,
                    "q",
                    key
                ));

                return cb(kbuf, ksiz, std::forward<TArgs&&>(args)...);
            }

        public:
            std::shared_ptr<WT_CURSOR> NewCursor(const char* const options = nullptr) {
                WT_CURSOR* cursor;
                AssertOk("Error opening cursor", Session_->open_cursor(
                    Session_.get(),
                    TableName(),
                    nullptr,
                    options,
                    &cursor
                ));

                return std::shared_ptr<WT_CURSOR>(cursor, [](WT_CURSOR* cursor) {
                    AssertOk("Failed to close cursor", cursor->close(cursor));
                });
            }

            TSession(
                DbWrapper& db,
                ESessionTable table,
                bool create = false
            )
                : Db(db)
                , Table(table)
            {
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
                        TableName(),
                        TableFormat()
                    ));
                }

                OwTCursor = NewCursor(((Table == ST_KV)
                    ? "overwrite=true,raw"
                    : "overwrite=true,raw,append"));

                OwFCursor = NewCursor(((Table == ST_KV)
                    ? "overwrite=false,raw"
                    : "overwrite=false,raw,append"));
            }

            // ~TSession() {
            //     OwTCursor->reset(OwTCursor.get());
            //     OwFCursor->reset(OwFCursor.get());
            // }

            bool GetUnpackedKey(WT_CURSOR& cursor, uint64_t* key) {
                WT_ITEM _key {
                    .data = nullptr,
                    .size = 0,
                };

                bool rv = (cursor.get_key(&cursor, &_key) == 0);

                if(rv) {
                    rv = (wiredtiger_struct_unpack(
                        Session_.get(),
                        _key.data,
                        _key.size,
                        "q",
                        key
                    ) == 0);

                    // if(rv) {
                    //     Utils::cluck(2, "GetUnpackedKey() success: key=%llu", *key);
                    // }
                }

                if(!rv) {
                    if(_key.data != nullptr) {
                        Utils::PrintString((unsigned char*)_key.data, _key.size);
                    }

                    Utils::cluck(2, "GetUnpackedKey() failed: key of size %lu", _key.size);

                    rv = false;
                }

                return rv;
            }

            bool add(
                const char * kbuf,
                size_t ksiz,
                const char * vbuf,
                size_t vsiz
            ) {
                if(Table != ST_KV) {
                    Utils::cluck(1, "add() is only allowed for ST_KV");
                    abort();
                }

#ifdef SKREE_DBWRAPPER_DEBUG
                Utils::PrintString(kbuf, ksiz);
                Utils::PrintString(vbuf, vsiz);
                Utils::cluck(1, "[add] ^");
#endif

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

            bool append(
                uint64_t* key,
                const char * vbuf,
                size_t vsiz
            ) {
                if(Table != ST_QUEUE) {
                    Utils::cluck(1, "append() is only allowed for ST_QUEUE");
                    abort();
                }

                WT_ITEM value {
                    .data = vbuf,
                    .size = vsiz,
                };

                OwFCursor->set_value(OwFCursor.get(), &value);

                bool rv = (OwFCursor->insert(OwFCursor.get()) == 0);

                if(rv) {
                    rv = GetUnpackedKey(*OwFCursor, key);
                }

                return rv;
            }

            bool set(
                const char * kbuf,
                size_t ksiz,
                const char * vbuf,
                size_t vsiz
            ) {
                if(Table != ST_KV) {
                    Utils::cluck(1, "set() is only allowed for ST_KV");
                    abort();
                }

#ifdef SKREE_DBWRAPPER_DEBUG
                Utils::PrintString(kbuf, ksiz);
                Utils::PrintString(vbuf, vsiz);
                Utils::cluck(1, "[set] ^");
#endif

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
#ifdef SKREE_DBWRAPPER_DEBUG
                Utils::PrintString(kbuf, ksiz);
                Utils::cluck(1, "[remove] ^");
#endif

                WT_ITEM key {
                    .data = kbuf,
                    .size = ksiz,
                };

                OwTCursor->set_key(OwTCursor.get(), &key);

                return (OwTCursor->remove(OwTCursor.get()) == 0);
            }

            bool remove(const uint64_t key) {
                return WithPackedKey<bool>(key, [this](const char* kbuf, const size_t ksiz) {
                    return remove(kbuf, ksiz);
                });
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

                if(value.size() != ovsiz) {
                    // Utils::cluck(3, "[cas] size mismatch: %llu != %llu", value.size(), ovsiz);
                    // abort();
                    return false;
                }

                if(strncmp(value.data(), ovbuf, ovsiz) != 0) {
                    Utils::cluck(1, "[cas] data mismatch");
                    return false;
                }

                auto result = set(kbuf, ksiz, nvbuf, nvsiz);

                if(!result) {
                    Utils::cluck(1, "[cas] set() failed");
                }

                if(!end_transaction(result)) {
                    Utils::cluck(1, "[cas] failed to commit transaction");
                    return false;
                }

                return result;
            }

            template<typename... TArgs>
            bool cas(const uint64_t key, TArgs... args) {
                return WithPackedKey<bool>(key, [this](
                    const char* kbuf,
                    const size_t ksiz,
                    TArgs... args
                ) {
                    return cas(kbuf, ksiz, std::forward<TArgs>(args)...);
                }, std::forward<TArgs>(args)...);
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
#ifdef SKREE_DBWRAPPER_DEBUG
                Utils::PrintString(kbuf, ksiz);
                Utils::cluck(1, "[get] ^");
#endif

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

            std::string get(const uint64_t key) {
                return WithPackedKey<std::string>(key, [this](const char* kbuf, const size_t ksiz) {
                    return get(kbuf, ksiz);
                });
            }

            int32_t check(
                const char * kbuf,
                size_t ksiz
            ) {
                return get(kbuf, ksiz).size();
            }

            int32_t check(const uint64_t key) {
                return WithPackedKey<int32_t>(key, [this](const char* kbuf, const size_t ksiz) {
                    return check(kbuf, ksiz);
                });
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

        class TReader {
        private:
            std::shared_ptr<TSession> Session;
            std::shared_ptr<WT_CURSOR> Cursor;

        public:
            using TData = WT_ITEM;

            TReader(DbWrapper& db) {
                Session = db.NewSession(DbWrapper::TSession::ST_QUEUE);
                Cursor = Session->NewCursor("raw,readonly");
            }

            bool Read(uint64_t* key, TData& value) {
                int result = Cursor->next(Cursor.get());

                if(result != 0) {
                    if(result == WT_NOTFOUND) {
                        Reset();

                    } else {
                        Utils::cluck(
                            2,
                            "read failed: %s",
                            wiredtiger_strerror(result)
                        );
                    }

                    return false;
                }

                if(!Session->GetUnpackedKey(*Cursor, key)) {
                    return false;
                }

                Cursor->get_value(Cursor.get(), &value);

                return true;
            }

            bool StepBack() {
                int result = Cursor->prev(Cursor.get());

                if(result != 0) {
                    if(result == WT_NOTFOUND) {
                        Reset();

                    } else {
                        Utils::cluck(
                            2,
                            "stepback failed: %s",
                            wiredtiger_strerror(result)
                        );
                    }

                    return false;
                }

                return true;
            }

            bool Reset() {
                int result = Cursor->reset(Cursor.get());

                if(result != 0) {
                    Utils::cluck(
                        2,
                        "reset failed: %s",
                        wiredtiger_strerror(result)
                    );

                    return false;
                }

                return true;
            }
        };

        std::shared_ptr<TReader> NewReader() {
            return std::make_shared<TReader>(*this);
        }

    private:
        std::shared_ptr<WT_CONNECTION> Db;
        std::shared_ptr<Utils::MappedFile> PkFile;

    public:
        DbWrapper(std::string&& dbFileName);

        std::shared_ptr<TSession> NewSession(
            TSession::ESessionTable table = TSession::ST_KV,
            bool create = false
        ) {
            return std::make_shared<TSession>(*this, table, create);
        }

        void create() {
            NewSession(TSession::ST_KV, true);
            NewSession(TSession::ST_QUEUE, true);
        }

        bool add(
            const char * kbuf,
            size_t ksiz,
            const char * vbuf,
            size_t vsiz
        ) {
            return NewSession()->add(kbuf, ksiz, vbuf, vsiz);
        }

        bool append(
            uint64_t* key,
            const char * vbuf,
            size_t vsiz
        ) {
            return NewSession(TSession::ST_QUEUE)->append(key, vbuf, vsiz);
        }

        bool set(
            const char * kbuf,
            size_t ksiz,
            const char * vbuf,
            size_t vsiz
        ) {
            return NewSession()->set(kbuf, ksiz, vbuf, vsiz);
        }

        bool remove(const char * kbuf, size_t ksiz) {
            return NewSession()->remove(kbuf, ksiz);
        }

        bool remove(const uint64_t key) {
            return NewSession(TSession::ST_QUEUE)->remove(key);
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

        bool cas(
            const uint64_t key,
            const char * ovbuf,
            size_t ovsiz,
            const char * nvbuf,
            size_t nvsiz
        ) {
            return NewSession(TSession::ST_QUEUE)->cas(key, ovbuf, ovsiz, nvbuf, nvsiz);
        }

        bool synchronize() {
            PkFile->sync();
            return true;
        }

        std::string get(const char * kbuf, size_t ksiz) {
            return NewSession()->get(kbuf, ksiz);
        }

        std::string get(const uint64_t key) {
            return NewSession(TSession::ST_QUEUE)->get(key);
        }

        int32_t check(const char * kbuf, size_t ksiz) {
            return NewSession()->get(kbuf, ksiz).size();
        }

        int32_t check(const uint64_t key) {
            return NewSession(TSession::ST_QUEUE)->get(key).size();
        }
    };
}
