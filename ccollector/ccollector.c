
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>

#include <pthread.h>

#include <hiredis/hiredis.h>


typedef struct redis_config_s {
  char* socket;
  char* ip;
  int   port;
} redis_config_t;

typedef struct config_s {
  int            port;
  int            verbose;
  redis_config_t redis;
} config_t;


#include "config.h"


#define BUFLEN (1024*3) // We don't expect packets bigger than this.


typedef struct keyval_s {
  char*            name;
  struct keyval_s* next;

  double data;
  int    samples;
} keyval_t;


typedef struct data_s {
  keyval_t*       keys;
  pthread_mutex_t mutex;
  pthread_t       thread;
} data_t;


data_t seconds;
data_t minutes;
data_t hours;


redisContext *redis;


keyval_t* getkey(data_t* data, char* name) {
  keyval_t* k = data->keys;

  while (k != 0) {
    if (strcmp(k->name, name) == 0) {
      return k;
    }

    k = k->next;
  }

  return 0;
}


keyval_t* addkey(data_t* data, char* name, int dup) {
  keyval_t* k = (keyval_t*)malloc(sizeof(keyval_t));

  if (dup) {
    k->name  = strdup(name);
  } else {
    k->name  = name;
  }

  k->data    = 0;
  k->samples = 0;

  k->next    = data->keys;
  data->keys = k;

  return k;
}


void process(data_t* what, data_t* into, char ws) {
  pthread_mutex_lock(&what->mutex);

  if (into) {
    pthread_mutex_lock(&into->mutex);
  }

  keyval_t* k = what->keys;

  while (k != 0) {
    double value = 0;

    if (k->samples > 0) {
      value = k->data / k->samples;
    }

    k->data = k->samples = 0;

    char command[1024];

    snprintf(command, 1024, "RPUSH %s:%c %.2f", k->name, ws, value);
    freeReplyObject(redisCommand(redis, command));

    snprintf(command, 1024, "LTRIM %s:%c -288 -1", k->name, ws);
    freeReplyObject(redisCommand(redis, command));

    if (into != 0) {
      keyval_t* ki = getkey(into, k->name);

      if (ki == 0) {
        ki = addkey(into, k->name, 0);
      }

      ki->data += value;
      ++ki->samples;
    }

    k = k->next;
  }

  pthread_mutex_unlock(&what->mutex);

  if (into) {
    pthread_mutex_unlock(&into->mutex);
  }
}


void* process_seconds(void* id) {
  for (;;) {
    sleep(5);

    process(&seconds, &minutes, 's');
  }
}


void* process_minutes(void* id) {
  for (;;) {
    sleep(5 * 60);

    process(&minutes, &hours, 'm');
  }
}


void* process_hours(void* id) {
  for (;;) {
    sleep(60 * 60);

    process(&hours, 0, 'h');
  }
}


void processkey(char* buf) {
  char* val = strstr(buf, ":");

  if (val == 0) {
    printf("invalid data [%s]\n", buf);
    return;
  }

  *(val++) = 0;


  pthread_mutex_lock(&seconds.mutex);

  keyval_t* k = getkey(&seconds, buf);

  if (k == 0) {
    printf("new key: %s\n", buf);
    k = addkey(&seconds, buf, 1);
  }
  

  char* sep    = strstr(val, "|");
  double value = 0;

  if (sep != 0) {
    *(sep++) = 0;

    value = strtod(val, 0);

    if (*sep == 'c') {
      if (k->samples != 5) {
        k->samples = 5;
      }
    } else {
      printf("invalid data type: [%c]\n", *sep);
      value = 0;
    }
  } else {
    value = strtod(val, 0);

    ++k->samples;
  }

  k->data += value;
  
  pthread_mutex_unlock(&seconds.mutex);
}


int main() {
  struct sockaddr_in si_me, si_other;
  int s, l;
  socklen_t slen = sizeof(si_other);
  char buf[BUFLEN + 1];

  if ((s = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) < 0) {
    printf("creating socket failed\n");
    return 1;
  }

  memset((void*)&si_me, 0, sizeof(si_me));

  si_me.sin_family = AF_INET;
  si_me.sin_port = htons(config.port);
  si_me.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(s, (struct sockaddr*)&si_me, sizeof(si_me)) < 0) {
    printf("bind failed\n");
    return 2;
  }



  if (config.redis.socket == 0) {
    redis = redisConnect(config.redis.ip, config.redis.port);
  } else {
    redis = redisConnectUnix(config.redis.socket);
  }

  if (redis->err) {
    printf("redis failed: %s\n", redis->errstr);
    return 3;
  }


  pthread_mutex_init(&seconds.mutex, 0);
  pthread_mutex_init(&minutes.mutex, 0);
  pthread_mutex_init(&hours.mutex  , 0);


  pthread_create(&seconds.thread, 0, process_seconds, 0);
  pthread_create(&minutes.thread, 0, process_minutes, 0);
  pthread_create(&hours.thread  , 0, process_hours  , 0);


  for (;;) {
    l = recvfrom(s, buf, BUFLEN, 0, (struct sockaddr*)&si_other, &slen);
   
    if (l == -1) {
      printf("recvfrom failed\n");
      return 4;
    }

    buf[l] = 0; // recvfrom doesn't zero terminate

    int len = strlen(buf);
    int stt = 0;

    int i;
    for (i = 0; i < len; ++i) {
      if (buf[i] == ',') {
        buf[i] = 0;

        processkey(&buf[stt]);

        stt = i + 1;
      }
    }

    processkey(&buf[stt]);
  }

  close(s);

  return 0;
}
