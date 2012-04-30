'use strict';

var config = require('./config.js');
var redis  = require('redis-node');
var dgram  = require('dgram');


var redisClient = redis.createClient(config.redis.port, config.redis.ip);


var stats = {
  'seconds': {},
  'minutes': {},
  'hours'  : {}
};


dgram.createSocket('udp4', function(data) {
  // Ignore empty messages (they somehow sometimes happen).
  if (data.length == 0) {
    return;
  }

  data = data.toString('utf8').split(',');

  for (var i = 0; i < data.length; ++i) {
    var d = data[i].split(':');

    if (d.length != 2) {
      console.log('ERROR: invalid data "' + data[i] + '"');
      continue;
    }

    var key = d[0] + ':s';
    var dtp = d[1].split('|'); // data|type pair
    var val = parseFloat(dtp[0]);
      
    if (!stats.seconds[key]) {
      console.log('INFO: new key ' + key.substr(0, key.length - 2));

      stats.seconds[key] = {data: 0, samples: 0};
    }

    if (dtp.length == 1) { // data point
      ++stats.seconds[key].samples;
    } else if (dtp[1] == 'c') { // counter
      if (stats.seconds[key].samples != 5) {
        stats.seconds[key].samples = 5;
      }
    } else {
      console.log('ERROR: Invalid data type ' + dtp[1]);
      val = 0;
    }
      
    stats.seconds[key].data += val;
  }
}).bind(config.port || 9876, config.ip);
  


function process(what, into) {
  for (var key in stats[what]) {
    var value = 0;

    if (stats[what][key].samples > 0) {
      value = Math.round((stats[what][key].data / stats[what][key].samples) * 100) / 100;
    }

    stats[what][key].data    = 0;
    stats[what][key].samples = 0;

    redisClient.rpush(key, value);
    redisClient.ltrim(key, -288, -1);

    if (into !== false) {
      // transform key:s into key:m
      key = key.substr(0, key.length - 1) + into.substr(0, 1);

      if (!stats[into][key]) {
        stats[into][key] = {data: 0, samples: 0};
      }

      stats[into][key].data += value;
      ++stats[into][key].samples;
    }
  }
}


setInterval(process.bind(this, 'seconds', 'minutes'),       5 * 1000); // store our second average each 5 seconds
setInterval(process.bind(this, 'minutes', 'hours'  ),  5 * 60 * 1000); // store our minute average each 5 minute
setInterval(process.bind(this, 'hours'  , false    ), 60 * 60 * 1000); // store our hour   average each 1 hour

