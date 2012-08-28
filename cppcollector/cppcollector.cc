
#include <iostream> // cout, sync_with_stdio
#include <fstream>  // ifstream

#include <map>

#include <netinet/in.h> // sockaddr_in
#include <stdlib.h>     // strtod
#include <string.h>     // strchr
#include <errno.h>      // errno

#include <boost/thread.hpp>
#include <boost/date_time.hpp>

#include <hiredis/hiredis.h>

#include "jsoncpp/include/json/json.h"


// Default values
#define CONFIG_PORT         9876
#define CONFIG_REDIS_SOCKET "redis.sock"
#define CONFIG_REDIS_IP     "127.0.0.1"
#define CONFIG_REDIS_PORT   6377

#define BUFLEN (1024*3) // We don't expect packets bigger than this.


using namespace std;


class counter {
public:
  double data;
  int    samples;

  counter() {
    data    = 0;
    samples = 0;
  }
};

typedef map<string, counter> countermap;

typedef struct timeframe {
  countermap   counters;
  boost::mutex mutex;
} timeframe;


timeframe seconds;
timeframe minutes;
timeframe hours;

redisContext *redis;

Json::Value config;


void process(timeframe* what, timeframe* into, char ws, int timeout) {
  for (;;) {
    boost::posix_time::seconds nextrun(timeout);
    boost::this_thread::sleep(nextrun);

    // open a scope for our scoped mutex
    {
      boost::mutex::scoped_lock wl(what->mutex);

      if (into != NULL) {
        into->mutex.lock();
      }
      
      countermap::iterator i;

      for (i = what->counters.begin(); i != what->counters.end(); ++i) {
        counter* c = &(*i).second;

        double value = 0;

        if (c->samples > 0) {
          value = c->data / c->samples;
        }

        c->data = c->samples = 0;

        char command[1024];

        snprintf(command, 1024, "RPUSH %s:%c %.2f", (*i).first.c_str(), ws, value);
        freeReplyObject(redisCommand(redis, command));

        snprintf(command, 1024, "LTRIM %s:%c -288 -1", (*i).first.c_str(), ws);
        freeReplyObject(redisCommand(redis, command));

        if (into != NULL) {
          counter* ic;
          countermap::iterator ii = into->counters.find((*i).first);

          if (ii == into->counters.end()) {
            pair<countermap::iterator, bool> r = into->counters.insert(pair<string, counter>((*i).first, counter()));

            ic = &(*r.first).second;
          } else {
            ic = &(*ii).second;
          }

          ic->data += value;
          ++ic->samples;
        }
      }

      if (into != NULL) {
        into->mutex.unlock();
      }
    } // close of the mutex scope
  }
}


void processkey(char* buf) {
  char* val = strchr(buf, ':');

  if (val == 0) {
    cout << "invalid data [" << buf << "]" << endl;
    return;
  }

  *(val++) = 0;


  {
    boost::mutex::scoped_lock sl(seconds.mutex);

    counter* c;
    countermap::iterator i = seconds.counters.find(buf);

    if (i == seconds.counters.end()) {
      cout << "new key: " << buf << endl;

      pair<countermap::iterator, bool> r = seconds.counters.insert(pair<string, counter>(buf, counter()));

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
        cout << "invalid data type: [" << *sep << "]" << endl;
        value = 0;
      }
    } else {
      value = strtod(val, 0);

      ++c->samples;
    }

    c->data += value;
  }
}


bool parse_config() {
  Json::Reader reader;
  ifstream     file("config.js");

  if (!reader.parse(file, config)) {
    cout << "parsing config.js failed: " << reader.getFormattedErrorMessages();
    return false;
  }

  if (!config.isMember("redis")) {
    cout << "redis config is missing!" << endl;
    return false;
  }

  return true;
}


int main() {
  struct sockaddr_in si_me, si_other;
  int s, l;
  socklen_t slen = sizeof(si_other);
  char buf[BUFLEN + 1];

  ios::sync_with_stdio(false);

  if (!parse_config()) {
    // parse_config will output it's own error.
    return 1;
  }

  if ((s = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0) {
    cout << "creating socket failed" << endl;
    return 2;
  }

  memset((void*)&si_me, 0, sizeof(si_me));

  si_me.sin_family = AF_INET;
  si_me.sin_port = htons(config.get("port", CONFIG_PORT).asInt());
  si_me.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(s, (struct sockaddr*)&si_me, sizeof(si_me)) < 0) {
    cout << "bind failed: " << strerror(errno) << endl;
    return 3;
  }


  if (config["redis"].isMember("socket")) {
    redis = redisConnect(config["redis"].get("ip", CONFIG_REDIS_IP).asCString(), config["redis"].get("port", CONFIG_REDIS_PORT).asInt());
  } else {
    redis = redisConnectUnix(config["redis"].get("socket", CONFIG_REDIS_SOCKET).asCString());
  }

  if (redis->err) {
    cout << "redis failed: " << redis->errstr << endl;
    return 4;
  }


  boost::thread seconds_thread(process, &seconds, &minutes        , 's',       5);
  boost::thread minutes_thread(process, &minutes, &hours          , 'm',  5 * 60);
  boost::thread hours_thread  (process, &hours  , (timeframe*)NULL, 'h', 60 * 60); // Just passing NULL confuses the boost::thread template to much.


  for (;;) {
    l = recvfrom(s, buf, BUFLEN, 0, (struct sockaddr*)&si_other, &slen);
   
    if (l == -1) {
      cout << "recvfrom failed" << endl;
      return 4;
    }

    buf[l] = 0; // recvfrom doesn't zero terminate

    int len = strlen(buf);
    int stt = 0;

    for (int i = 0; i < len; ++i) {
      if (buf[i] == ',') {
        buf[i] = 0;

        processkey(&buf[stt]);

        stt = i + 1;
      }
    }

    processkey(&buf[stt]);
  }

  // This will never be reached so we don't have to .join() our threads.
}

