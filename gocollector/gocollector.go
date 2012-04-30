package main

import (
  "strings";
  "strconv";
  "fmt";

  "net";

  "time";
  "sync";

  "io/ioutil";
  "encoding/json";

  "./godis";
)


// The JSON config struct
type Config struct {
  Port    int
  Redis   RedisConfig
}

type RedisConfig struct {
  Socket string
}


// Data counter, holds the data for the current time frame
type Counter struct {
  data    float64
  samples int
}

type TimeFrame struct {
  counters map[string] *Counter // Pointers seem to be just a bit faster then reassigning the value again and again
  lock     sync.Mutex // Maps are not thread safe
}


// Our Redis connection
var redis *godis.Client;


var seconds = TimeFrame{map[string] *Counter{}, sync.Mutex{}}
var minutes = TimeFrame{map[string] *Counter{}, sync.Mutex{}}
var hours   = TimeFrame{map[string] *Counter{}, sync.Mutex{}}


func process(what, into *TimeFrame, ws string, sleep time.Duration) {
  // Just keep on looping and sleep every cycle
  for ;; {
    time.Sleep(sleep * 1e9)

    what.lock.Lock()

    if into != nil {
      into.lock.Lock()
    }

    // Loop through all values and store them in Redis
    for k,v := range what.counters {
      var value float64 = 0

      if v.samples > 0 {
        value = v.data / float64(v.samples)
      }

      v.data    = 0
      v.samples = 0

      redis.Rpush(k + ws, strconv.FormatFloat(value, 'f', 2, 64))
      redis.Ltrim(k + ws, -288, -1) // Trim the list to 288 values

      // Add the sample to the higher up counter
      if into != nil {
        p, ok := into.counters[k]

        if !ok {
          c := Counter{0, 0}

          into.counters[k] = &c
          p = &c
        }

        p.data += value;
        p.samples++
      }
    }

    what.lock.Unlock()

    if into != nil {
      into.lock.Unlock()
    }
  }
}


func main() {
  fmt.Println("collector started")


  file, err := ioutil.ReadFile("config.json")
  if err != nil {
    fmt.Println(err)
    return
  }


  config := new(Config)

  // Parse the JSON config file
  err = json.Unmarshal(file, config)
  if err != nil {
    fmt.Println(err)
    return
  }


  redis = godis.New("unix:" + config.Redis.Socket, 0, "")


  addr, err := net.ResolveUDPAddr("udp", ":" + strconv.Itoa(config.Port))
  if  err != nil {
    fmt.Println(err)
    return
  }

  c, err := net.ListenUDP("udp", addr)
  if err != nil {
    fmt.Println(err)
    return
  }


  // These will run in different threads
  go process(&seconds, &minutes, ":s",       5)
  go process(&minutes, &hours  , ":m",  5 * 60)
  go process(&hours  , nil     , ":h", 60 * 60)


  var buffer [1024*3] byte;

  for ;; {
    bytes, _, err := c.ReadFrom(buffer[0:])
    if err != nil {
      fmt.Println(err)
      continue
    }


    // Skip empty messages
    if bytes == 0 {
      continue
    }


    // Seperate the statements
    statements := strings.Split(string(buffer[0:bytes]), ",")

    for i := 0; i < len(statements); i++ {
      // Seperate the key from the value
      keyval := strings.Split(statements[i], ":")

      if len(keyval) != 2 {
        fmt.Printf("ERROR: invalid data \"%v\"\n", statements[i]);
        continue
      }

      key      := keyval[0]
      dpt      := strings.Split(keyval[1], "|")
      val, err := strconv.ParseFloat(dpt[0], 64)

      if err != nil {
        fmt.Println(err)
        continue
      }

      seconds.lock.Lock()

      p, ok := seconds.counters[key]

      if !ok {
        fmt.Printf("INFO: new key %v\n", key)

        c := Counter{0, 0}

        seconds.counters[key] = &c
        p = &c
      }

      if len(dpt) == 2 {
        if dpt[1] == "c" {
          p.samples = 5
        } else {
          fmt.Printf("ERROR: invalid data type [%v]\n", dpt[1])
          continue
        }
      } else {
        p.samples++
      }

      p.data += val

      seconds.lock.Unlock()
    }
  }
}
