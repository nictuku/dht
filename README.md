This is a golang Kademlia/Bittorrent DHT library that implements [BEP
5](http://www.bittorrent.org/beps/bep_0005.html).

It's typically used by a torrent client such as
[Taipei-Torrent](http://github.com/nictuku/Taipei-Torrent), but it could also
be used by a standalone DHT routers, or for other more creative purposes.

The DHT performs well and supports the most important features despite its
simple API.

It's able to process approx 3000 incoming packets per second in a single core of
a very old AMD Athlon(tm) 64 Processor 3700+, when the optional rate-limiting
feature is disabled.

For usage details and examples, see the online documentation at:
http://go.pkgdoc.org/github.com/nictuku/dht

