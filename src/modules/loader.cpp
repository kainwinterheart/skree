#include <unistd.h>
#include <pthread.h>
#include "../skree/workers/fork_manager.hpp"
#include "../skree/utils/fork_manager.hpp"
#include "../skree/utils/events.hpp"

int main() {
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    size_t stacksize = 0;
    pthread_attr_getstacksize(&attr, &stacksize);
    printf("Thread stack size = %d bytes\n", stacksize);

    auto* module = new Skree::Utils::skree_module_t();
    const char* path = "/Users/gennadiy/Yandex.Disk.localized/skree/src/modules/base/base.so";

    module->path_len = strlen(path);
    module->config = nullptr; // TODO

    module->path = (char*)malloc(module->path_len + 1);
    memcpy(module->path, path, module->path_len);
    module->path[module->path_len] = '\0';

    (new Skree::Workers::TForkManager(new Skree::Utils::TForkManager(3, module)))->start(); // TODO
    sleep(60);

    return 0;
}
