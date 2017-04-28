#pragma once

#include "utils/misc.hpp"
#include "utils/mapped_file.hpp"
#include <wiredtiger.h>
#include <utility>
// #include <pthread.h>
// #include <vector>
// #include <string>

// struct WT_SESSION_IMPL {
//     WT_SESSION iface;
//
//     void    *lang_private;      /* Language specific private storage */
//
//     uint32_t active;           /* Non-zero if the session is in-use */
//
//     const char *name;       /* Name */
//     const char *lastop;     /* Last operation */
//     uint32_t id;            /* UID, offset in session array */
// };

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
                    ? "key_format=u,value_format=u,split_pct=100,internal_page_max=16KB,leaf_page_max=16KB,leaf_value_max=64MB"
                    : "key_format=r,value_format=u,split_pct=100,internal_page_max=16KB,leaf_page_max=16KB,leaf_value_max=64MB");
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

            virtual ~TSession() {
                // #ifdef SKREE_DBWRAPPER_DEBUG
                // Utils::cluck(2, "Session %llu destroyed", (uint64_t)this);
                // #endif
                // OwTCursor->reset(OwTCursor.get());
                // OwFCursor->reset(OwFCursor.get());
                // fprintf(stderr, "Destroy session %d(%lx)\n", ((WT_SESSION_IMPL*)Session_.get())->id, (unsigned long)Session_.get());
            }

            TSession(
                DbWrapper& db,
                ESessionTable table,
                bool create = false
            )
                : Db(db)
                , Table(table)
            {
                #ifdef SKREE_DBWRAPPER_DEBUG
                Utils::cluck(2, "Session %llu created", (uint64_t)this);
                #endif

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

                // Utils::cluck(2, "Create session %d(%lx)", ((WT_SESSION_IMPL*)Session_.get())->id, (unsigned long)Session_.get());

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
                }

                return rv;
            }

            void Sync() {
                AssertOk("log_flush() failed", Session_->log_flush(Session_.get(), "sync=off"));
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
                #ifdef SKREE_DBWRAPPER_DEBUG
                Utils::cluck(2, "Append at session %llu", (uint64_t)this);
                #endif

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
                if(!begin_transaction()) {
                    Utils::cluck(1, "[cas] failed to begin transaction");
                    return false;
                }

                auto commit = [this](bool commit, unsigned short step) {
                    if(end_transaction(commit)) {
                        return true;

                    } else {
                        Utils::cluck(2, "[cas] failed to commit transaction; step = %hu", step);
                        return false;
                    }
                };

                const auto& value = get(kbuf, ksiz);

                if(value.size() != ovsiz) {
                    // Utils::cluck(3, "[cas] size mismatch: %llu != %llu", value.size(), ovsiz);
                    // abort();
                    commit(false, 1);
                    return false;
                }

                if(strncmp(value.data(), ovbuf, ovsiz) != 0) {
                    Utils::cluck(1, "[cas] data mismatch");
                    commit(false, 2);
                    return false;
                }

                auto result = set(kbuf, ksiz, nvbuf, nvsiz);

                if(!result) {
                    Utils::cluck(1, "[cas] set() failed");
                }

                if(!commit(result, 3)) {
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
                #ifdef SKREE_DBWRAPPER_DEBUG
                Utils::cluck(2, "Transaction started at session %llu", (uint64_t)this);
                #endif
                return (Session_->begin_transaction(Session_.get(), "snapshot") == 0);
            }

            bool end_transaction(bool commit = true) {
                int rv;
                if(commit) {
                    #ifdef SKREE_DBWRAPPER_DEBUG
                    Utils::cluck(2, "Transaction committed at session %llu", (uint64_t)this);
                    #endif
                    rv = Session_->commit_transaction(Session_.get(), nullptr);

                } else {
                    #ifdef SKREE_DBWRAPPER_DEBUG
                    Utils::cluck(2, "Transaction rolled back at session %llu", (uint64_t)this);
                    #endif
                    rv = Session_->rollback_transaction(Session_.get(), nullptr);
                }

                if (rv == 0) {
                    return true;

                } else {
                    Utils::cluck(1, wiredtiger_strerror(rv));
                    return false;
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

            std::shared_ptr<Utils::muh_str_t> next(uint64_t& key) {
#ifdef SKREE_DBWRAPPER_DEBUG
                Utils::cluck(1, "[next]");
#endif

                std::shared_ptr<Utils::muh_str_t> out;
                int result = OwFCursor->next(OwFCursor.get());

                if(result != 0) {
                    if(result != WT_NOTFOUND) {
                        Utils::cluck(
                            2,
                            "read failed: %s",
                            wiredtiger_strerror(result)
                        );
                    }

                    return out;
                }

                if(!GetUnpackedKey(*OwFCursor, &key)) {
                    return out;
                }

                WT_ITEM value;
                AssertOk("Failed to fetch data", OwFCursor->get_value(OwFCursor.get(), &value));

                out.reset(new Utils::muh_str_t(
                    (char*)malloc((uint32_t)value.size), (uint32_t)value.size, true
                ));

                memcpy(out->data, value.data, out->len);

                return out;
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

            void Reset() {
                AssertOk("Failed to reset the session", Session_->reset(Session_.get()));
            }
        };

        class TReader {
        private:
            std::shared_ptr<TSession> Session;
            std::shared_ptr<WT_CURSOR> Cursor;

        public:
            using TData = WT_ITEM;

            TReader(DbWrapper& db) {
                // Session = db.NewSession(DbWrapper::TSession::ST_QUEUE);
                // Cursor = Session->NewCursor("raw,readonly");
            }

            bool Read(uint64_t* key, TData& value) {
                int result = Cursor->next(Cursor.get());

                if(result != 0) {
                    if(result == WT_NOTFOUND) {
                        // Reset();

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

                // Cursor->get_key(Cursor.get(), key);
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
                Session->Reset();
                // int result = Cursor->reset(Cursor.get());
                //
                // if(result != 0) {
                //     Utils::cluck(
                //         2,
                //         "reset failed: %s",
                //         wiredtiger_strerror(result)
                //     );
                //
                //     return false;
                // }

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
