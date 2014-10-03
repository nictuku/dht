// Runs a node on a random UDP port that attempts to collect 10 peers for an
// infohash, then keeps running as a passive DHT node.
//
// IMPORTANT: if the UDP port is not reachable from the public internet, you
// may see very few results.
//
// To collect 10 peers, it usually has to contact some 1k nodes. It's much easier
// to find peers for popular infohashes. This process is not instant and should
// take a minute or two, depending on your network connection.
//
//
// There is a builtin web server that can be used to collect debugging stats
// from http://localhost:8711/debug/vars.
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/spikebike/dht"
)

const (
	httpPortTCP = 8711
	numTarget   = 10
	exampleIH   = "deca7a89a1dbdc4b213de1c0d5351e92582f31fb" // ubuntu-12.04.4-desktop-amd64.iso
)

func main() {
	conf := dht.NewConfig()
// kona
	conf.Address = "[2607:f810:c20:1d:ca60:ff:fec8:76df]"
// shell
// conf.Address = "[2607:f810:c20:1c:a800:ff:fe23:f52f]"
// left
// conf.Address = "[2601:c:a200:1248:76d0:2bff:fe90:8b90]"

//	conf.Address = "128.120.246.131"
	conf.Port = 8445;
	// IPv6
	conf.DHTRouters = "[2001:41d0:1:aa81:2::1]:6881,[2601:0:8600:94d:78a2:a8da:11d7:5df4]:49438,[2002:6e16:48be:e472:290:a9ff:fe36:a203]:51413,[2a02:1810:b00f:b400:211:32ff:fe0e:46d5]:6881,[2001:41d0:1:dad1::1]:51413,[2600:3c03::f03c:91ff:fedb:19ed]:51413,[2001:41d0:2:1221::1]:59020,[2001:41d0:8:e2d6::1]:51413,[2607:f810:c20:1c:a800:ff:fe23:f52f]:8445,[2601:c:a200:1248:76d0:2bff:fe90:8b90]:8445"
	conf.UdpProto = "udp6"
//	conf.DHTRouters = "[2607:f810:c20:1c:a800:ff:fe23:f52f]:8445"
	// ipv4
//	conf.DHTRouters = "91.121.60.42:6881,212.129.33.50:6881,82.221.103.244:6881,67.215.246.10:6881"


	flag.Parse()
	// To see logs, use the -logtostderr flag and change the verbosity with
	// -v 0 (less verbose) up to -v 5 (more verbose).
	if len(flag.Args()) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %v <infohash>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example infohash: %v\n", exampleIH)
		flag.PrintDefaults()
		os.Exit(1)
	}
	ih, err := dht.DecodeInfoHash(flag.Args()[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "DecodeInfoHash error: %v\n", err)
		os.Exit(1)
	}
	// Starts a DHT node with the default options. It picks a random UDP port. To change this, see dht.NewConfig.
	d, err := dht.New(conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "New DHT error: %v", err)
		os.Exit(1)

	}
	// For debugging.
	go http.ListenAndServe(fmt.Sprintf(":%d", httpPortTCP), nil)

	go d.Run()
	go drainresults(d)

	for {
		d.PeersRequest(string(ih), false)
		time.Sleep(5 * time.Second)
	}
}

// drainresults loops, printing the address of nodes it has found.
func drainresults(n *dht.DHT) {
	count := 0
	fmt.Println("=========================== DHT")
	fmt.Println("Note that there are many bad nodes that reply to anything you ask.")
	fmt.Println("Peers found:")
	for r := range n.PeersRequestResults {
		for _, peers := range r {
			for _, x := range peers {
				fmt.Printf("%d: %v\n", count, dht.DecodePeerAddress(x))
				count++
				if count >= numTarget {
					os.Exit(0)
				}
			}
		}
	}
}
