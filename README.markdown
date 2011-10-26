Red Cluster
===========

Red Cluster is a library that cluster's together a group of Redis servers together, presenting them to the outside world as though they are all just one redis server. If you are familiar with efforts being made by the redis team on building this cluster into redis, then none of this should be new.

Motivation
==========
The benefits of clustering have been well documented, but highlighting the most salient points, would be:

* Utilize the multiple cores machines have nowadays, while not sacrificing the speed you have grown to love about Redis.
* Add robustness to your data, by using replica sets. Having slaves for a write master. Automatic promotion of a slave to a master in the event of a failure, with zero downtime.

Setup
=====
If you are familiar with [MongoDB's replica sets](http://www.mongodb.org/display/DOCS/Replica+Sets) then this stuff will seem like deja-vu. Red Cluster sets up your redis servers, as shown below:

                                    
![Overview](https://img.skitch.com/20111026-e3w8stdemf33gyj8ciqauxpnyj.png)

There are one or more replica sets, each of which contain one write master and one or more read slaves.

Routing
=======
When you issue a redis command Red Cluster, it calculates the crc32 of the key you are looking to work with to figure out which replica set to forward the request to. Within the Replica Set itself, all read requests are sent to slaves while write requests are sent to the master.

Death of a master
=================
In the unfortunate event, that you have a master die in a replica set, the replica set self-heals and nominates and promotes a slave in that replica set to become the new master. Other slaves in the same replica set, are now made slaves of the new master.
