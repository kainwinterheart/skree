#include "fork_manager.hpp"
#include "../utils/fork_manager.hpp"

namespace Skree {
    namespace Workers {
        void TForkManager::run() {
            ((Utils::TForkManager*)args)->Start();
        }
    }
}
