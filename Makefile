CXX = $(HOME)/llvm/llvm_cmake_build/bin/clang++

MAKEFILE_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

INCLUDE_FLAGS := -nostdinc \
-I$(HOME)/llvm/llvm_cmake_build/lib/clang/5.0.0/include/ \
-I$(HOME)/llvm/llvm_cmake_build/include \
-I$(HOME)/llvm/llvm_cmake_build/include/c++/v1 \
-I/usr/local/include/ \
-I$(MAKEFILE_DIR)/contrib-build/usr/local/include \
-I$(MAKEFILE_DIR)/contrib-build/usr/include \
-I$(MAKEFILE_DIR)/contrib-build/include \
-I/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX10.11.sdk/usr/include

LIBRARY_FLAGS := -L/usr/local/lib \
-L$(MAKEFILE_DIR)/contrib-build/usr/local/lib \
-L$(MAKEFILE_DIR)/contrib-build/usr/lib \
-L$(MAKEFILE_DIR)/contrib-build/lib \
-L$(HOME)/llvm/llvm_cmake_build/lib \
-L$(HOME)/llvm/llvm_cmake_build/lib/clang/5.0.0/lib/darwin

CXXFLAGS = $(INCLUDE_FLAGS) \
-std=c++1z -Wno-expansion-to-defined \
-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free \
-g -fno-omit-frame-pointer -stdlib=libc++ \
# -DSKREE_LONGMESS \
# -DSKREE_DBWRAPPER_DEBUG \
# -fstack-protector-all \
# -fsanitize=address \
# -fsanitize=undefined -fno-sanitize=vptr \

LDFLAGS = $(LIBRARY_FLAGS) \
-g \
-Wl,-no_pie \
# -fsanitize=address \
# -fsanitize=undefined -fno-sanitize=vptr \
# -Wl,--export-dynamic \

OBJS = src/skree/utils/spin_lock.o src/skree/utils/hashers.o src/skree/utils/string.o src/skree/utils/events.o src/skree/utils/fork_manager.o src/skree/utils/round_robin_vector.o src/skree/utils/atomic_hash_map.o src/skree/utils/misc.o src/skree/utils/muhev.o main.o src/skree/actions/n.o src/skree/actions/c.o src/skree/actions/e.o src/skree/actions/h.o src/skree/actions/i.o src/skree/actions/l.o src/skree/actions/r.o src/skree/actions/w.o src/skree/actions/x.o src/skree/base/action.o src/skree/base/pending_read.o src/skree/base/pending_write.o src/skree/base/worker.o src/skree/client.o src/skree/db_wrapper.o src/skree/meta/opcodes.o src/skree/pending_reads/replication/ping_task.o src/skree/pending_reads/replication/propose_self.o src/skree/pending_reads/replication.o src/skree/server.o src/skree/workers/client.o src/skree/workers/discovery.o src/skree/workers/replication.o src/skree/workers/replication_failover.o src/skree/workers/synchronization.o src/skree/workers/statistics.o src/skree/queue_db.o src/skree/workers/processor.o src/skree/workers/processor_failover.o src/skree/workers/cleanup.o src/skree/meta/states.o src/skree/utils/string_sequence.o
# OBJS = src/skree/queue_db.o

LIBS = -lpthread -lstdc++ -lm -lyaml-cpp -ldl -lwiredtiger -lprofiler -ltcmalloc # -lclang_rt.ubsan_osx_dynamic

TARGET = build/skree

$(TARGET): $(OBJS)
	mkdir -p build
	$(CXX) -o $(TARGET) $(OBJS) $(LIBS) $(LDFLAGS)
	rm -f $(OBJS)

all: $(TARGET)

clean:
	rm -f $(OBJS) $(TARGET)

clean_all: clean_contrib clean

install:
	mkdir -p /usr/bin
	mv $(TARGET) /usr/bin/

contrib: contrib_yaml_cpp contrib_gperftools contrib_wiredtiger

contrib_yaml_cpp:
	mkdir -p contrib-build
	cd contrib/yaml-cpp && cmake . \
	&& make -j 16 && make DESTDIR=$(MAKEFILE_DIR)/contrib-build install

contrib_gperftools:
	mkdir -p contrib-build
	cd contrib/gperftools && ./autogen.sh && \
	./configure --prefix=$(MAKEFILE_DIR)/contrib-build \
	&& make -j 16 && make install

contrib_wiredtiger:
	mkdir -p contrib-build
	cd contrib/wiredtiger && ./autogen.sh && \
	CC="${HOME}/llvm/llvm_cmake_build/bin/clang" \
	CFLAGS="$(INCLUDE_FLAGS) -Wno-expansion-to-defined" \
	LDFLAGS="$(LIBRARY_FLAGS)" \
	./configure \
		--prefix=$(MAKEFILE_DIR)/contrib-build \
		--enable-verbose \
		--enable-tcmalloc \
		--with-spinlock=gcc \
	&& make -j 16 && make install

clean_contrib: clean_contrib_yaml_cpp clean_contrib_gperftools clean_contrib_wiredtiger
	rm -rf contrib-build

clean_contrib_yaml_cpp:
	cd contrib/yaml-cpp && make DESTDIR=$(MAKEFILE_DIR)/contrib-build uninstall ||:
	cd contrib/yaml-cpp && make DESTDIR=$(MAKEFILE_DIR)/contrib-build clean ||:
	rm contrib/yaml-cpp/CMakeCache.txt ||:

clean_contrib_gperftools:
	cd contrib/gperftools && make uninstall ||:
	cd contrib/gperftools && make clean ||:

clean_contrib_wiredtiger:
	cd contrib/wiredtiger && make uninstall ||:
	cd contrib/wiredtiger && make clean ||:
