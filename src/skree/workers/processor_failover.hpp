#pragma once

#include "processor.hpp"

namespace Skree {
    namespace Workers {
        class ProcessorFailover : public Skree::Workers::Processor {
        public:
            ProcessorFailover(Skree::Server& _server, void* _args = nullptr)
                : Skree::Workers::Processor(_server, _args) {}
        private:
            virtual uint32_t process(const uint64_t& now, Utils::known_event_t& event) const override;
            virtual void Stat(Utils::known_event_t& event, uint32_t count) const override;
        };
    }
}

// #include "../server.hpp" // sorry
