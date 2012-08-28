
#include <stdio.h>  // fprintf
#include <stdlib.h> // malloc
#include <string.h> // strlen, strchr

#include <fstream>  // ifstream
#include <map>

#include <hiredis/hiredis.h>

#include "libuv/include/uv.h"
#include "jsoncpp/include/json/json.h"


#define CHECK(action) if (action) { uv_err_t err = uv_last_error(uv_loop); fprintf(stderr, "%s:%d:%s() %s\n", __FILE__, __LINE__, __FUNCTION__, uv_strerror(err)); return 1; }


// Default values
#define CONFIG_PORT         9876
#define CONFIG_REDIS_SOCKET "redis.sock"
#define CONFIG_REDIS_IP     "127.0.0.1"
#define CONFIG_REDIS_PORT   6377


using namespace std;


typedef struct counter {
  double data;
  int    samples;
} counter;

typedef map<string, counter> countermap;


typedef struct process_data {
  countermap* what;
  countermap* into;
  char       ws;
} process_data;


typedef struct work {
  uv_work_t request;
  uv_buf_t  buf;
} work;


static uv_loop_t* uv_loop;
static uv_udp_t   server;

countermap seconds;
countermap minutes;
countermap hours;

redisContext *redis;

Json::Value config;




static void process_cb(uv_timer_t* handle, int status) {
  process_data* data = (process_data*)handle->data;

  countermap::iterator i;
  countermap::iterator end = data->what->end();

  for (i = data->what->begin(); i != end; ++i) {
    counter* c = &(*i).second;

    double value = 0;

    if (c->samples > 0) {
      value = c->data / c->samples;
    }

    c->data = c->samples = 0;

    char command[1024];

    snprintf(command, 1024, "RPUSH %s:%c %.2f", (*i).first.c_str(), data->ws, value);
    freeReplyObject(redisCommand(redis, command));

    snprintf(command, 1024, "LTRIM %s:%c -288 -1", (*i).first.c_str(), data->ws);
    freeReplyObject(redisCommand(redis, command));

    if (data->into != NULL) {
      counter* ic;
      countermap::iterator ii = data->into->find((*i).first);

      if (ii == data->into->end()) {
        counter newcounter = {0, 0};
        pair<countermap::iterator, bool> r = data->into->insert(pair<string, counter>((*i).first, newcounter));

        ic = &(*r.first).second;
      } else {
        ic = &(*ii).second;
      }

      ic->data += value;
      ++ic->samples;
    }
  }
}


void processkey(char* buf) {
  char* val = strchr(buf, ':');

  if (val == 0) {
    fprintf(stderr, "%s:%d:%s() invalid data [%s]\n", __FILE__, __LINE__, __FUNCTION__, buf);
    return;
  }

  *(val++) = 0;


  counter* c;
  countermap::iterator i = seconds.find(buf);

  if (i == seconds.end()) {
    printf("new key: [%s]\n", buf);

    counter newcounter = {0, 0};
    pair<countermap::iterator, bool> r = seconds.insert(pair<string, counter>(buf, newcounter));

    c = &(*r.first).second;
  } else {
    c = &(*i).second;
  }

  char* sep    = strchr(val, '|');
  double value = 0;

  if (sep != 0) {
    *(sep++) = 0;

    value = strtod(val, 0);

    if (*sep == 'c') {
      if (c->samples != 5) {
        c->samples = 5;
      }
    } else {
      fprintf(stderr, "%s:%d:%s() invalid data type [%s]\n", __FILE__, __LINE__, __FUNCTION__, sep);
      value = 0;
    }
  } else {
    value = strtod(val, 0);

    ++c->samples;
  }

  c->data += value;
}


static uv_buf_t alloc_cb(uv_handle_t* handle, size_t suggested_size) {
  uv_buf_t buf;
  buf.base = (typeof(buf.base))malloc(suggested_size + 1); // Just to make sure our 0 terminator fits in there
  buf.len = suggested_size;
  return buf;
}


static void sv_recv_cb(uv_udp_t* handle, ssize_t nread, uv_buf_t buf, struct sockaddr* addr, unsigned flags) {
  if (nread > 0) {
    buf.base[nread] = 0; // Make sure to 0 terminate since we use it as a string

    int len = strlen(buf.base); // Could use nread here but that won't work if someone sends a message with \0 bytes in it
    int stt = 0;

    for (int i = 0; i < len; ++i) {
      if (buf.base[i] == ',') {
        buf.base[i] = 0;

        processkey(&buf.base[stt]);

        stt = i + 1;
      }
    }

    processkey(&buf.base[stt]);
  } else {
    // Empty message or end of message (or error?)
  }
    
  free(buf.base);
}


int parse_config() {
  Json::Reader reader;
  ifstream     file("config.js");

  if (!reader.parse(file, config)) {
    fprintf(stderr, "%s:%d:%s %s\n", __FILE__, __LINE__, __FUNCTION__, reader.getFormattedErrorMessages().c_str());
    return 1;
  }

  if (!config.isMember("redis")) {
    fprintf(stderr, "%s:%d:%s redis config key missing\n", __FILE__, __LINE__, __FUNCTION__);
    return 1;
  }

  return 0;
}


int main() {
  // Parse the config file
  if (parse_config()) {
    // parse_config will output it's own error.
    return 1;
  }


  // Make a connection to Redis
  if (config["redis"].isMember("socket")) {
    redis = redisConnect(config["redis"].get("ip", CONFIG_REDIS_IP).asCString(), config["redis"].get("port", CONFIG_REDIS_PORT).asInt());
  } else {
    redis = redisConnectUnix(config["redis"].get("socket", CONFIG_REDIS_SOCKET).asCString());
  }

  if (redis->err) {
    fprintf(stderr, "%s:%d:%s %s\n", __FILE__, __LINE__, __FUNCTION__, redis->errstr);
    return 1;
  }


  // Get a pointer to our event loop
  uv_loop = uv_default_loop();


  // Run on 4 threads
  //eio_set_min_parallel(4);


  // Initialize the timers used to push the data into Redis
  uv_timer_t   seconds_timer;
  uv_timer_t   minutes_timer;
  uv_timer_t   hours_timer;

  // This function will never return so it doesn't matter if these are local variables
  process_data seconds_data = {&seconds, &minutes, 's'};
  process_data minutes_data = {&minutes, &hours  , 'm'};
  process_data hours_data   = {&hours  , NULL    , 'h'};

  seconds_timer.data = &seconds_data;
  minutes_timer.data = &minutes_data;
  hours_timer.data   = &hours_data;

  CHECK(uv_timer_init(uv_loop, &seconds_timer))
  CHECK(uv_timer_init(uv_loop, &minutes_timer))
  CHECK(uv_timer_init(uv_loop, &hours_timer))

  CHECK(uv_timer_start(&seconds_timer, process_cb,       5 * 1000,       5 * 1000))
  CHECK(uv_timer_start(&minutes_timer, process_cb,  5 * 60 * 1000,  5 * 60 * 1000))
  CHECK(uv_timer_start(&hours_timer  , process_cb, 60 * 60 * 1000, 60 * 60 * 1000))


  // Setup the UDP listening server
  CHECK(uv_udp_init(uv_loop, &server))

  struct sockaddr_in address = uv_ip4_addr("0.0.0.0", config.get("port", CONFIG_PORT).asInt());

  CHECK(uv_udp_bind(&server, address, 0))

  uv_udp_recv_start(&server, alloc_cb, sv_recv_cb);


  // Start the main event loop.
  // This function should not return.
  uv_run(uv_loop);
}

