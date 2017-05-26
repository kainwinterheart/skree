#pragma once

#include <functional>
#include <utility>

namespace Skree {
    namespace Utils {
        struct TCharPointerComparator : public std::function<bool(
            const char*, const char*
        )> {
            inline bool operator()(const char* a, const char* b) const {
                return (strcmp(a, b) == 0);
            }
        };

        //BKDR hash algorithm
        static inline int CharPointerHash(size_t len, const char* key) {
            const int seed = 131; //31 131 1313 13131131313 etc//
            uint64_t hash = 0;

            for(size_t i = 0; i < len; ++i) {
                hash = ((hash * seed) + key[i]);
            }

            return (hash & 0x7FFFFFFF);
        }

        struct TCharPointerHasher {
            inline int operator()(const char* key) const {
                return CharPointerHash(strlen(key), key);
            }
        };

        template<typename T>
        struct TPairHasher {
            inline int operator()(const std::pair<T, T> pair) const {
                std::hash<T> int_hasher;
                return int_hasher(pair.first) ^ int_hasher(pair.second);
            }
        };
    }
}
