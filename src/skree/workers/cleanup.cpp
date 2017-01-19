#include "cleanup.hpp"

namespace Skree {
    namespace Workers {
        void Cleanup::run() {
            while(true) {
                sleep(1);
            }
        }
    }
}
