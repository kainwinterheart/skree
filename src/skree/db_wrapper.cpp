#include "db_wrapper.hpp"
#include <rocksdb/merge_operator.h>
#include <atomic>

namespace Skree {
    class TMergeOp : public rocksdb::AssociativeMergeOperator {
    public:
        enum EOp {
            OP_MIN = 0,
            OP_CAS = 1,
            OP_MAX = 2,
        };

    private:
        struct TOpSpec {
            const EOp Op;
            const char* const Data;
            const size_t Size;
        };

        static inline TOpSpec Deserialize(const rocksdb::Slice& input) {
            const auto* const data = input.data();

            if(!((data[0] > OP_MIN) && (data[0] < OP_MAX))) {
                Utils::cluck(2, "Invalid op: 0x%X", (unsigned char)data[0]);
                abort();
            }

            return TOpSpec {
                .Op = (EOp)data[0],
                .Data = (data + 1),
                .Size = (input.size() - 1),
            };
        }

    public:
        TMergeOp() : rocksdb::AssociativeMergeOperator() {
        }

        virtual bool Merge(
            const rocksdb::Slice& key,
            const rocksdb::Slice* existing_value,
            const rocksdb::Slice& value,
            std::string* new_value,
            rocksdb::Logger* logger
        ) const override {
            const auto& spec = Deserialize(value);

            switch(spec.Op) {
                case OP_CAS: {
                        if(!existing_value) {
                            return false;
                        }

                        uint64_t pos = 0;

                        const uint64_t orig_len = *(uint64_t*)spec.Data;
                        pos += sizeof(orig_len);

                        const char* orig = spec.Data + pos;
                        pos += orig_len;

                        const uint64_t nval_len = *(uint64_t*)(spec.Data + pos);
                        pos += sizeof(nval_len);

                        const char* nval = spec.Data + pos;

                        if(
                            (existing_value->size() == orig_len)
                            && (strncmp(existing_value->data(), orig, orig_len))
                        ) {
                            new_value->assign(nval, nval_len);
                            return true;

                        } else {
                            return false;
                        }
                    }

                default:
                    abort();
            }
        }

        virtual const char* Name() const override {
            return "TMergeOp";
        }
    };

    DbWrapper::DbWrapper(std::string&& dbFileName) {
        rocksdb::DB* db;
        rocksdb::Options options;

        options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();

        options.create_if_missing = true;
        options.merge_operator.reset(new TMergeOp);
        options.compression = rocksdb::kNoCompression;
        options.compression_per_level.clear();

        rocksdb::Status status (rocksdb::DB::Open(options, dbFileName.c_str(), &db));

        if(!status.ok()) {
            Utils::cluck(2, "%s: %s", dbFileName.c_str(), status.ToString().c_str());
            abort();
        }

        Db.reset(db);

        dbFileName.append(".pk");

        PkFile.reset(new Utils::MappedFile(dbFileName.c_str(), sizeof(uint64_t)));
    }

    bool DbWrapper::add(
        const char * kbuf,
        size_t ksiz,
        const char * vbuf,
        size_t vsiz
    ) {
        // Utils::cluck(2, "db.add(%s)", kbuf);
        return Db->Put(
            rocksdb::WriteOptions(),
            rocksdb::Slice(kbuf, ksiz),
            rocksdb::Slice(vbuf, vsiz)
        ).ok();
    }

    bool DbWrapper::set(
        const char * kbuf,
        size_t ksiz,
        const char * vbuf,
        size_t vsiz
    ) {
        return add(kbuf, ksiz, vbuf, vsiz);
    }

    bool DbWrapper::remove(
        const char * kbuf,
        size_t ksiz
    ) {
        // fprintf(stderr, "db_wrapper::remove(");
        // for(size_t i = 0; i < ksiz; ++i) {
        //     fprintf(stderr, "%.2X", kbuf[i]);
        // }
        // Utils::cluck(1, ")");
        // abort();
        return Db->Delete(
            rocksdb::WriteOptions(),
            rocksdb::Slice(kbuf, ksiz)
        ).ok();
    }

    uint64_t DbWrapper::increment(uint64_t num) {
        static_assert(sizeof(std::atomic<uint64_t>) == sizeof(uint64_t), "sizeof(std::atomic<uint64_t>) == sizeof(uint64_t)");
        return std::atomic_fetch_add((std::atomic<uint64_t>*)PkFile->begin(), num) + num;
    }

    bool DbWrapper::cas(
        const char * kbuf,
        size_t ksiz,
        const char * ovbuf,
        size_t ovsiz,
        const char * nvbuf,
        size_t nvsiz
    ) {
        uint64_t pos = 1;
        const uint64_t vsiz = 1 + (sizeof(uint64_t) * 2) + ovsiz + nvsiz;
        char* vbuf = (char*)malloc(vsiz);

        vbuf[0] = TMergeOp::OP_CAS;

        memcpy(vbuf + pos, &ovsiz, sizeof(ovsiz));
        pos += sizeof(ovsiz);

        memcpy(vbuf + pos, ovbuf, ovsiz);
        pos += ovsiz;

        memcpy(vbuf + pos, &nvsiz, sizeof(nvsiz));
        pos += sizeof(nvsiz);

        memcpy(vbuf + pos, nvbuf, nvsiz);

        bool rv = Db->Merge(
            rocksdb::WriteOptions(),
            rocksdb::Slice(kbuf, ksiz),
            rocksdb::Slice(vbuf, vsiz)
        ).ok();

        free(vbuf);

        return rv;
    }

    bool DbWrapper::synchronize(bool hard) {
        PkFile->sync();
        return true;
    }

    bool DbWrapper::begin_transaction(bool hard) {
        // lock();
        // auto rv = kyotocabinet::HashDB::begin_transaction(hard);
        // // unlock();
        // return rv;
        return true;
    }

    bool DbWrapper::end_transaction(bool commit) {
        // // lock();
        // auto rv = kyotocabinet::HashDB::end_transaction(commit);
        // unlock();
        // return rv;
        return true;
    }

    std::string DbWrapper::get(
        const char * kbuf,
        size_t ksiz
    ) {
        std::string out;

        const auto& status = Db->Get(
            rocksdb::ReadOptions(),
            rocksdb::Slice(kbuf, ksiz),
            &out
        );

        if(!status.ok() && !status.IsNotFound()) {
            Utils::cluck(
                3,
                "DbWrapper::get(): bad key, code = %d, subcode = %d, msg = %s",
                status.code(),
                status.subcode(),
                status.ToString().c_str()
            );
        }

        return out;
    }

    int32_t DbWrapper::check(
        const char * kbuf,
        size_t ksiz
    ) {
        return get(kbuf, ksiz).size();
    }
}
