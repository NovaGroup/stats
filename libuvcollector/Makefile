
all: libuv/uv.a jsoncpp/libs/linux-gcc-4.5.2/libjson_linux-gcc-4.5.2_libmt.so libuvcollector.cc
	g++ -O2 -Wall -o libuvcollector libuvcollector.cc -lpthread -L/usr/local/lib/ -lhiredis -l:jsoncpp/libs/linux-gcc-4.5.2/libjson_linux-gcc-4.5.2_libmt.so libuv/out/Release/obj.target/libuv.a -lrt

libuv/uv.a:
	$(MAKE) -C libuv

jsoncpp/libs/linux-gcc-4.5.2/libjson_linux-gcc-4.5.2_libmt.so: jsoncpp/include/json/json.h
	scons -C jsoncpp platform="linux-gcc"

