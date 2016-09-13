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
                thread = std::make_shared<pthread_t>();
                run_args = std::make_shared<run_args_t>();

                run_args->cb = [this](){
                    run();
                };

                pthread_create(thread.get(), nullptr, __run, (void*)run_args.get());
            }

            virtual ~WorkerLite() {
                if(thread) {
                    pthread_join(*thread, nullptr);
                }
            }

            virtual void run() = 0;

        private:
            std::shared_ptr<pthread_t> thread;
            struct run_args_t {
                std::function<void()> cb;
            };
            std::shared_ptr<run_args_t> run_args;

            static void* __run(void* _args) {
                ((run_args_t*)_args)->cb();
                return nullptr;
            }
        };
    }
}
