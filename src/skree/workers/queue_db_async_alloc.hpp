#pragma once

#include "../base/worker_lite.hpp"

namespace Skree {
    namespace Workers {
        template<typename F>
        class QueueDbAsyncAlloc : public Skree::Base::WorkerLite {
        private:
            const F cb;
        public:
            QueueDbAsyncAlloc(const F& _cb, const void* _args = nullptr)
                : Skree::Base::WorkerLite(_args), cb(_cb) {}

            virtual void run() override {
                cb();
                delete this; // TODO?
            }
        };
    }
}
