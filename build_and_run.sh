#!/bin/sh

echo "rm -rf ~/kch/{7654,8765,9876} ; mkdir -p ~/kch/{7654,8765,9876} ; make clean ; make -j 16 && lldb ./build/skree -- --port=7654 --db=$HOME/kch/7654/ --events=./events.yaml --page-size=1"
