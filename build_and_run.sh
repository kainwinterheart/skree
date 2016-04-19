NO_FLAGS="" # -Wno-c++98-compat-pedantic -Wno-c++98-compat -Wno-exit-time-destructors -Wno-global-constructors -Wno-shadow -Wno-old-style-cast"
#MORE_FLAGS="-Wall -Weverything ${NO_FLAGS}"
MORE_FLAGS="-fwrapv"

FILES=$(find ./src -type f -name '*.cpp')

clang++ ${MORE_FLAGS} -I /usr/local/Cellar/libev/4.20/include/ -I /usr/local/include/ -L /usr/local/Cellar/libev/4.20/lib/ -L /usr/local/lib -std=c++11 main.cpp ${FILES} -lev -lpthread -lc -lz -lkyotocabinet -lstdc++ -lm -lyaml-cpp

if [ "$?" -eq "0" ]; then

    echo k
    # ./a.out --port=7654 --db=$HOME/kch/7654.kch --events=./events.yaml
    # ./a.out --port=8765 --db=$HOME/kch/8765.kch --events=./events.yaml
    # lldb ./a.out -- --port=7654 --db=$HOME/kch/7654.kch --events=./events.yaml
    # lldb ./a.out -- --port=8765 --db=$HOME/kch/8765.kch --events=./events.yaml
fi
