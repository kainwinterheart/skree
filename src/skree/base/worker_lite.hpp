#pragma once

#include <stdlib.h>
#include <pthread.h>
#include <functional>

namespace Skree {
    namespace Base {
        class WorkerLite {
        protected:
            const void* args;
        public:
            WorkerLite(const void* _args = nullptr) : args(_args) {
            }

            virtual void start() {
                thread = (pthread_t*)malloc(sizeof(*thread));

                run_args* args = new run_args {
                    .cb = [this](){
                        run();
                    }
                };

                pthread_create(thread, nullptr, __run, (void*)args);
            }

            virtual ~WorkerLite() {
                if(thread != nullptr) {
                    pthread_join(*thread, nullptr);
                    free(thread);
                }
            }

            virtual void run() = 0;
        private:
            pthread_t* thread;
            struct run_args {
                std::function<void()> cb;
            };

            static void* __run(void* _args) {
                run_args* args = ((run_args*)_args);
                args->cb();
                delete args;
                return nullptr;
            }
        };
    }
}
