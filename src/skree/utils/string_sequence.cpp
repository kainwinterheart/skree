#include "string_sequence.hpp"
#include "misc.hpp"

namespace Skree {
    namespace Utils {
        void StringSequence::concat(const std::shared_ptr<muh_str_t>& data) {
            memory.push_back(data);
            sequence.push_back(TItem {
                .len = data->len,
                .data = data->data
            });
        }
    }
}
