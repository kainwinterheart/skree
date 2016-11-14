#include "misc.hpp"

#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace Skree {
    namespace Utils {
        class MappedFile {
        private:
            int fh;
            char* addr;
            const size_t size;
        public:
            inline MappedFile(const char* path, const size_t _size)
                : size(_size)
            {
                if(
                    (access(path, O_RDWR) == -1)
                    && (Utils::alloc_file(path, size) != size)
                ) {
                    abort();
                }

                fh = open(path, O_RDWR);

                if(fh == -1) {
                    perror(path);
                    abort();
                }

                addr = (char*)mmap(
                    0,
                    size,
                    PROT_READ | PROT_WRITE,
                    MAP_FILE | MAP_SHARED,
                    fh,
                    0
                );

                if(addr == MAP_FAILED) {
                    perror("mmap");
                    abort();
                }
            }

            inline char* begin() {
                return addr;
            }

            inline void sync() {
                if(msync(addr, size, MS_SYNC) == -1) {
                    perror("msync");
                    // abort();
                }

                if(fsync(fh) == -1) {
                    perror("fsync");
                    // abort();
                }
            }
        };
    }
}
