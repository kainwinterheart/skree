#pragma once

#include <memory>

namespace Skree {
    namespace Utils {
        class StringSequence {
        private:
            std::shared_ptr<StringSequence> next;
            const char* data;
            size_t len;
        public:
            inline StringSequence(size_t _len, const char* _data)
                : data(_data)
                , len(_len)
            {
            }

            inline void reset(size_t _len, const char* _data) {
                len = _len;
                data = _data;
            }

            template<typename T>
            inline const char* read(size_t offset = 0, T* _len = nullptr, StringSequence** _next = nullptr) {
                StringSequence* node = this;

                while(offset >= node->len) {
                    if(node->next) {
                        offset -= node->len;
                        node = node->next.get();

                    } else {
                        throw std::logic_error ("Reading past the end of string sequence");
                    }
                }

                if(_len != nullptr)
                    *_len = node->len - offset;

                if(_next != nullptr)
                    *_next = node;

                return node->data + offset;
            }

            template<typename T>
            inline const char* read(size_t offset = 0, StringSequence** _next = nullptr, T* _len = nullptr) {
                return read(offset, _len, _next);
            }

            inline void concat(std::shared_ptr<StringSequence> _node) {
                StringSequence* node = this;

                while(node->next)
                    node = node->next.get();

                node->next = _node;
            }

            inline std::shared_ptr<StringSequence> get_next() {
                return next;
            }
        };
    }
}
