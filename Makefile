CXX = clang++

CXXFLAGS = -I /usr/local/Cellar/libev/4.20/include/ -I /usr/local/include/ -std=c++11
LDFLAGS = -L /usr/local/Cellar/libev/4.20/lib/ -L /usr/local/lib

OBJS = main.o

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
