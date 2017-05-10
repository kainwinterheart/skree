#pragma once

#include <functional>

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
    }
}
