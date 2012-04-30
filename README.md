
stats
=====

Simple UDP stats.

Config
------

Make sure to edit config.js

Data
----

You can send the following data to the collector:

Data can be a comma (,) seperated list of:

`key:val`  
a data point (assumes each point is the total increment over a second)

`key:inc|c`  
increment a counter by `inc`

`key:d`  
delete key

Once a new key is received it will automatically be added to the database and you'll be able to see it's graphs.


Collectors
----------

These are all the available collector implementations sorted by their efficiency. They were all tested on the same machine with the same workload (around 11000 UDP packets per second).

<table>
<tr><th>Name</th><th>RSS</th><th>CPU usage</th></tr>
<tr><td>C</td><td>624KB</td><td>1.2</td></tr>
<tr><td>C++</td><td>1664KB</td><td>1.3</td></tr>
<tr><td>libuv</td><td>1796KB</td><td>1.6</td></tr>
<tr><td>Go</td><td>2896KB</td><td>3.2</td></tr>
<tr><td>Nodejs</td><td>34MB</td><td>5.8</td></tr>
</table>

