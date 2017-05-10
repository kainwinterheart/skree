#pragma once

#include "hashers.hpp"
#include <memory>

namespace Skree {
    namespace Utils {
        struct muh_str_t {
            char* data;
            uint32_t len;
            bool own;
            std::shared_ptr<muh_str_t> origin; // TODO: this is shit

            muh_str_t()
                : data(nullptr)
                , len(0)
                , own(false)
            {
            }

            muh_str_t(
                char* _data,
                uint32_t _len,
                bool _own
            )
                : data(_data)
                , len(_len)
                , own(_own)
            {
            }

            muh_str_t(const muh_str_t& right) = delete;
            muh_str_t& operator=(const muh_str_t& right) = delete;

            muh_str_t(muh_str_t&& right)
                : data(right.data)
                , len(right.len)
                , own(right.own)
                , origin(std::move(right.origin))
            {
                right.own = false;
            }

            muh_str_t& operator=(muh_str_t&& right) {
                data = right.data;
                len = right.len;
                own = right.own;
                origin = std::move(right.origin);

                right.own = false;

                return *this;
            }

            ~muh_str_t() {
                if(own && data)
                    free(data);
            }
        };

        static inline std::shared_ptr<muh_str_t> NewStr(uint32_t len) {
            auto out = std::shared_ptr<muh_str_t>();
            out.reset(new muh_str_t((char*)malloc(len), len, true));

            return out;
        }

        struct TMuhStrPointerComparator : public std::function<bool(
            std::shared_ptr<muh_str_t>, std::shared_ptr<muh_str_t>
        )> {
            inline bool operator()(
                const std::shared_ptr<muh_str_t>& a,
                const std::shared_ptr<muh_str_t>& b
            ) const {
                return (strcmp(a->data, b->data) == 0);
            }
        };

        struct TMuhStrPointerHasher {
            inline int operator()(const std::shared_ptr<muh_str_t>& key) const {
                return CharPointerHash(key->len, key->data);
            }
        };
    }
}
