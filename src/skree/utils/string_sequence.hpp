#pragma once

namespace Skree {
    namespace Utils {
        class StringSequence {
        private:
            StringSequence* next;
            const char* data;
            size_t len;
        public:
            inline StringSequence(size_t _len, const char* _data)
                : data(_data)
                , len(_len)
                , next(nullptr)
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
                    if(node->next == nullptr) {
                        throw std::logic_error ("Reading past the end of string sequence");

                    } else {
                        offset -= node->len;
                        node = node->next;
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

            inline void concat(StringSequence* _node) {
                StringSequence* node = this;

                while(node->next != nullptr)
                    node = node->next;

                node->next = _node;
            }

            inline StringSequence* get_next() {
                return next;
            }
        };
    }
}
