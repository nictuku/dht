This is a Kademlia/Bittorrent DHT that implements BEP 5.

It's typically used by a torrent client such as
[Taipei-Torrent](http://github.com/nictuku/Taipei-Torrent), but it could also
be used by a standalone DHT routers, or for other more creative purposes.

The DHT performs well and supports the most important features despite its
simple API.

It's able to process 2500 incoming packets per second in a single core of a
very old AMD Athlon(tm) 64 Processor 3700+, when rate-limiting is disabled.

For usage details and examples, see the online documentation at:
http://go.pkgdoc.org/github.com/nictuku/dht

