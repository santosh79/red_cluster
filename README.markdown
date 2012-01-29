Red Cluster
===========

Red Cluster is a library that cluster's together a group of Redis servers, presenting them to the outside world as though they are all just one redis server. If you are familiar with efforts being made by the redis team on building this cluster into redis, then none of this should be new.

Motivation
----------
The benefits of clustering have been well documented, but highlighting the most salient points, would be:

* Utilize the multiple cores machines have nowadays, while not sacrificing the speed you have grown to love about Redis.
* Add robustness to your data, by using replica sets. Having slaves for a write master. Automatic promotion of a slave to a master in the event of a failure, with zero downtime.

Setup
-----
If you are familiar with [MongoDB's replica sets](http://www.mongodb.org/display/DOCS/Replica+Sets) then this stuff will seem like deja-vu. Red Cluster sets up your redis servers, as shown below:

                                    
![Overview](https://img.skitch.com/20111026-e3w8stdemf33gyj8ciqauxpnyj.png)

There are one or more replica sets, each of which contain one write master and one or more read slaves.

Routing
-------
When you issue a redis command Red Cluster, it calculates the crc32 of the key you are looking to work with to figure out which replica set to forward the request to. Within the Replica Set itself, all read requests are sent to slaves while write requests are sent to the master.

Death of a master
-----------------
In the unfortunate event, that you have a master die in a replica set, the replica set self-heals and nominates and promotes a slave in that replica set to become the new master. Other slaves in the same replica set, are now made slaves of the new master.

Usage
-----
### Installation
Including Red Cluster in your ruby project is done via rubygems:
    gem install red_cluster

### Setup
As shown, in the diagram above Red Cluster works with one more replica sets & this concept is translated into Ruby code much as you would expect:

``` ruby
first_replica_set = {
  :master => {:host => "localhost", :port => 6379}, 
  :slaves => [{:host => "localhost", :port => 7379},
    {:host => "localhost", :port => 8379}]
}
second_replica_set = {
  :master => {:host => "localhost", :port => 9379}, 
  :slaves => [{:host => "localhost", :port => 10379},
    {:host => "localhost", :port => 11379}]
}
third_replica_set = {
  :master => {:host => "localhost", :port => 12379}, 
  :slaves => [{:host => "localhost", :port => 13379},
    {:host => "localhost", :port => 14379}]
}
replica_sets = [first_replica_set, second_replica_set, third_replica_set]
rc = RedCluster.new replica_sets

rc.set "foo", "bar"
rc.incr "user_count"
```

### Loading in your existing data
Red Cluster supports loading in a Redis [AOF file](http://redis.io/topics/persistence#append-only-file). The code for doing this is as follows:

``` ruby
rc = RedCluster.new replica_sets
rc.load_aof_file "/path/to/file"
```

### Unsupported commands
This library is a work in progress, and since the distributed system challenges of clustering together a federation of redis servers is an arduous task, a couple of commands in the redis toolkit are not supported in the interest of getting this library out in haste. The list of unsupported ops are:

* auth
* discard
* watch
* object
* sort
* slaveof
* All of the pub sub commands
* All blocking commands

### Semi-supported commands
The set union, diff and intersection commands along with their sorted set cousins; do work 100% but are not going to be as performant as you would see if you were just using Redis directly. The reason being a lot of the heavy lifting is now being done in ruby land, instead of within the redis server to overcome the distributed nature of the problem.

### Thread Safety
This is not a thread safe library. In JRuby, synchronizing on the red cluster reference should make this safe to use in a multi-thread environment. In MRI since [parallelism is still a work in progress](http://www.engineyard.com/blog/2011/ruby-concurrency-and-you/) I'd recommend not even bothering & just have Thread local references.

Author
------

Santosh Kumar :: santosh79@gmail.com :: @santosh79

#### LICENSE

(The MIT License)
Copyright (c) 2009:
"Santosh Kumar"
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the 'Software'), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
0:0