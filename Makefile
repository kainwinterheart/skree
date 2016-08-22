CXX = clang++

CXXFLAGS = -I /usr/local/Cellar/libev/4.20/include/ -I /usr/local/include/ -I contrib-build/usr/local/include -I contrib-build/usr/lib -std=c++11 -fstack-protector-strong
LDFLAGS = -L /usr/local/Cellar/libev/4.20/lib/ -L /usr/local/lib -fsanitize=address -L contrib-build/usr/local/lib -L contrib-build/usr/lib
# LDFLAGS = -L /usr/local/Cellar/libev/4.20/lib/ -L /usr/local/lib -L contrib-build/usr/local/lib -L contrib-build/usr/lib

OBJS = main.o src/skree/actions/n.o src/skree/actions/c.o src/skree/actions/e.o src/skree/actions/h.o src/skree/actions/i.o src/skree/actions/l.o src/skree/actions/r.o src/skree/actions/w.o src/skree/actions/x.o src/skree/base/action.o src/skree/base/pending_read.o src/skree/base/pending_write.o src/skree/base/worker.o src/skree/client.o src/skree/db_wrapper.o src/skree/meta/opcodes.o src/skree/pending_reads/replication/ping_task.o src/skree/pending_reads/replication/propose_self.o src/skree/pending_reads/replication.o src/skree/server.o src/skree/utils/misc.o src/skree/workers/client.o src/skree/workers/discovery.o src/skree/workers/replication.o src/skree/workers/synchronization.o src/skree/queue_db.o src/skree/workers/processor.o src/skree/workers/cleanup.o src/skree/meta/states.o
# OBJS = src/skree/queue_db.o

LIBS = -lev -lpthread -lc -lz -lkyotocabinet -lstdc++ -lm -lyaml-cpp -ldl

TARGET = build/skree

$(TARGET): contrib $(OBJS)
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

contrib: contrib_yaml_cpp

contrib_yaml_cpp:
	mkdir -p contrib-build
	cd contrib/yaml-cpp && cmake . && make -j 16 && make DESTDIR=../../contrib-build install

clean_contrib: clean_contrib_yaml_cpp
	rm -rf contrib-build

clean_contrib_yaml_cpp:
	cd contrib/yaml-cpp && make DESTDIR=../../contrib-build uninstall && make clean ||:
