CXX = clang++

CXXFLAGS = -I /usr/local/Cellar/libev/4.20/include/ -I /usr/local/include/ -std=c++11
LDFLAGS = -L /usr/local/Cellar/libev/4.20/lib/ -L /usr/local/lib -fsanitize=address

OBJS = main.o src/skree/actions/c.o src/skree/actions/e.o src/skree/actions/h.o src/skree/actions/i.o src/skree/actions/l.o src/skree/actions/r.o src/skree/actions/w.o src/skree/actions/x.o src/skree/base/action.o src/skree/base/pending_read.o src/skree/base/pending_write.o src/skree/base/worker.o src/skree/client.o src/skree/db_wrapper.o src/skree/meta/opcodes.o src/skree/pending_reads/noop.o src/skree/pending_reads/replication/ping_task.o src/skree/pending_reads/replication/propose_self.o src/skree/pending_reads/replication.o src/skree/server.o src/skree/utils/misc.o src/skree/workers/client.o src/skree/workers/discovery.o src/skree/workers/replication.o src/skree/workers/replication_exec.o src/skree/workers/synchronization.o

LIBS = -lev -lpthread -lc -lz -lkyotocabinet -lstdc++ -lm -lyaml-cpp

TARGET = build/skree

$(TARGET): $(OBJS)
	mkdir -p build
	$(CXX) -o $(TARGET) $(OBJS) $(LIBS) $(LDFLAGS)
	rm -f $(OBJS)

all: $(TARGET)

clean:
	rm -f $(OBJS) $(TARGET)

install:
	mkdir -p /usr/bin
	mv $(TARGET) /usr/bin/
