#include "cleanup.hpp"

namespace Skree {
    namespace Workers {
        void Cleanup::run() {
            while(true) {
                sleep(1);

                for(const auto& it : server.known_events) {
                    const auto& event = *(it.second);

                    cleanup_queue(*(event.queue));
                    cleanup_queue(*(event.queue2));
                    cleanup_queue(*(event.r_queue));
                    cleanup_queue(*(event.r2_queue));
                }
            }
        }

        void Cleanup::cleanup_queue(const Skree::QueueDb& queue) const {
            auto num = queue.get_first_used_page_num();
            auto path_len = queue.get_path_len();
            auto path = queue.get_path();
            char file [path_len + 1 + 21 + 1];
            std::stack<char*> files;

            while(num > 0) {
                --num;

                memcpy(file, path, path_len);
                file[path_len] = '/';
                sprintf(file + path_len + 1, "%llu", num);

                if(access(file, R_OK) == 0) {
                    files.push(strdup(file));

                } else {
                    break;
                }
            }

            while(!files.empty()) {
                char* file = files.top();

                if(unlink(file) == -1){
                    perror("unlink");
                    break;
                }

                files.pop();
                free(file);
            }

            while(!files.empty()) {
                char* file = files.top();
                files.pop();
                free(file);
            }
        }
    }
}
