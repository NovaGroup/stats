
all: jsoncpp/libs/linux-gcc-4.5.2/libjson_linux-gcc-4.5.2_libmt.so
	g++ cppcollector.cc -O3 -Wall -L/usr/local/lib/ -lhiredis -lboost_thread-mt -lpthread -l:jsoncpp/libs/linux-gcc-4.5.2/libjson_linux-gcc-4.5.2_libmt.so -o cppcollector

jsoncpp/libs/linux-gcc-4.5.2/libjson_linux-gcc-4.5.2_libmt.so: jsoncpp/include/json/json.h
	scons -C jsoncpp platform="linux-gcc"

