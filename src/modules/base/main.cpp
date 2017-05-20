#include <stdio.h>
#include <cstdint>

__attribute__((visibility("default")))
extern "C" void init(const void*) {
    printf("init\n");
}

__attribute__((visibility("default")))
extern "C" void destroy() {
    printf("destroy\n");
}

__attribute__((visibility("default")))
extern "C" bool run(uint32_t numEvents, void* data) {
    printf("main: %lu\n", numEvents);
    return true;
}
