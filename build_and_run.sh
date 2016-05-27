#NO_FLAGS="" # -Wno-c++98-compat-pedantic -Wno-c++98-compat -Wno-exit-time-destructors -Wno-global-constructors -Wno-shadow -Wno-old-style-cast"
#MORE_FLAGS="-Wall -Weverything ${NO_FLAGS}"
#MORE_FLAGS="-fwrapv"

#FILES="" # $(find ./src -type f -name '*.cpp')

make clean ; make

if [ "$?" -eq "0" ]; then

    echo k
    # ./build/skree --port=7654 --db=$HOME/kch/7654/ --events=./events.yaml
    # ./build/skree --port=8765 --db=$HOME/kch/8765/ --events=./events.yaml
    # lldb ./build/skree -- --port=7654 --db=$HOME/kch/7654/ --events=./events.yaml
    # lldb ./build/skree -- --port=8765 --db=$HOME/kch/8765/ --events=./events.yaml
fi
