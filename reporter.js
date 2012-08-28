'use strict';

var http   = require('http');
var os     = require('os');
var fs     = require('fs');
var dns    = require('dns');
var net    = require('net');
var stats  = require(process.cwd() + '/statsapi.js');
var config = require(process.cwd() + '/config.js');

var hostname = os.hostname();


process.on('uncaughtException', function (err) {
 console.log(err);
});



stats.bind(config.collector);



// Store previous values so we can send the difference.
var previousValues = {};

function sendDataPoints(data) {
  var message = [];

  for (var key in data) {
    // When it's an array we don't send the difference, we just send the value.
    if (data[key] instanceof Array) {
      message.push(key + '.' + hostname + ':' + data[key][0]);
    } else {
      // Send the difference between data points.
      // The first call will send nothing and only store the points.
      var value = parseFloat(data[key]);

      if (typeof previousValues[key] != 'undefined') {
        var diff = value - previousValues[key];

        if (diff > 0) {
          message.push(key + '.' + hostname + ':' + diff);
        }
      }

      previousValues[key] = value;
    }
  }

  if (message.length == 0) {
    return;
  }

  message = new Buffer(message.join(','));

  stats.send(message);
}






// Collect lighttpd fcgi stats
if (config.php) { !function() {
  var options = {
    host  : config.php.host || '127.0.0.1',
    port  : config.php.port || 80,
    path  : config.php.path || '/server-statistics',
    method: 'GET'
  };

  setInterval(function() {
    http.request(options, function(res) {
      var data = '';

      res.on('data', function(chunk) {
        data += chunk;
      });

      res.on('end', function() {
        data = data.split('\n');

        if (!data[7]) {
          data = null; // Free memory
          return;
        }

        var overloaded = data[5].split(': ')[1];
        var served     = data[7].split(': ')[1];

        sendDataPoints({
          'php.served'    : served,
          'php.overloaded': overloaded
        });
      });
    }).end();
  }, 1000);
}(); }




// Collect redis stats
if (config.redis) { !function() {
  setInterval(function() {
    var client;

    if (config.redis.sock) {
      client = net.createConnection(config.redis.sock);
    } else {
      client = net.createConnection(config.redis.port, config.redis.host);
    }

    client.write('*1\r\n$4\r\ninfo\r\n');

    client.on('data', function(data) {
      data = data.toString('ascii');
      data = data.split('\n');

      for (var i = 0; i < data.length; ++i) {
        if (data[i].indexOf('total_commands_processed:') == 0) {
          sendDataPoints({
            'redis.commands': data[i].substr(25) // 25 = strlen('total_commands_processed:')
          });

          break;
        }
      }

      client.end();
    });
  }, 1000);
}(); }




// Collecte memory stats
setInterval(function() {
  fs.readFile('/proc/meminfo', function(err, content) {
    content = content.toString('utf8').split('\n');

    var info = {};

    for (var i = 0; i < content.length; ++i) {
      var line = content[i].split(/\s+/);

      info[line[0].replace(':', '')] = parseInt(line[1]);
    }

    sendDataPoints({
      'os.load': [os.loadavg()[0]],
      'os.free': [Math.round((info.MemFree + info.Cached) / 1024)]
    });
  });
}, 1000);




// Collect disk io stats
/*setInterval(function() {
  fs.readFile('/proc/diskstats', function(err, content) {
    content = content.toString('utf8').split('\n');

    var line = content[content.length - 2].split(/\s+/);

    sendDataPoints({
      'os.disk.read'   : line[4],
      'os.disk.written': line[8]
    });
  });
}, 1000);*/




// Collect network stats
setInterval(function() {
  fs.readFile('/proc/net/dev', function(err, content) {
    content  = content.toString('utf8').split('\n');

    var data = {};

    for (var i = 3; i < content.length - 1; ++i) {
      var line = content[i].split(/\s+/);
      var eth  = line[1].replace(':', '');

      data['os.net.read.'    + eth] = line[2];
      data['os.net.written.' + eth] = line[10];
    }

    sendDataPoints(data);
  });
}, 1000);




console.log('reported started');

