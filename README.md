This is a golang Kademlia/Bittorrent DHT library that implements [BEP
5](http://www.bittorrent.org/beps/bep_0005.html).

It's typically used by a torrent client such as
[Taipei-Torrent](http://github.com/jackpal/Taipei-Torrent), but it could also be used by a
standalone DHT routers, or for other more creative purposes.

The DHT performs well and supports the most important features despite its simple API.

A multi-node deployment is able to process more than 5000 incoming packets per second in a single
core of a very old AMD Athlon(tm) 64 Processor 3700+, when the optional rate-limiting feature is
disabled.

![Performance stats](https://lh5.googleusercontent.com/-fqWcxpm2L5k/UzMd1MXYjfI/AAAAAAABJrM/huYWTUBPAA4/w928-h580-no/perf.png)

By default, if left running for several days the DHT node should use approx. 30MB of RAM. This can
be adjusted by changing MaxInfoHashes and MaxInfoHashPeers accordingly.

For usage details, see the online documentation at:
http://godoc.org/github.com/nictuku/dht

A full example is at:
[find_infohash_and_wait](examples/find_infohash_and_wait/main.go)

[![Build Status](https://drone.io/github.com/nictuku/dht/status.png)](https://drone.io/github.com/nictuku/dht/latest)

Google+ Community
-----------------

We have a [Taipei-Torrent Google+ Community](https://plus.google.com/u/0/communities/100997865549971977580) for the parent project [Taipei Torrent](https://github.com/jackpal/Taipei-Torrent) and also this DHT library. 
