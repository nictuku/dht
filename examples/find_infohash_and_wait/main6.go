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

	"github.com/nictuku/dht"
)

const (
	httpPortTCP = 8711
	numTarget   = 10
	exampleIH   = "deca7a89a1dbdc4b213de1c0d5351e92582f31fb" // ubuntu-12.04.4-desktop-amd64.iso
)

func main() {
	ipv6Address := flag.String("v6", "", "Address to bind to IPv6 interface")
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

	conf4 := dht.NewConfig()
	conf4.UDPProto = "udp4"
	conf4.Port = 8445
	// standard IPv4 bootstrap nodes = dht.transmissionbt.com
	// router.utorrent.com router.bittorrent.com
	conf6 := dht.NewConfig()
	conf6.UDPProto = "udp6"
	conf6.Port = 8445
	conf6.Address = *ipv6Address
	// Starts a DHT node with the default options. It picks a random UDP port. To change this, see dht.NewConfig.
	d4, err := dht.New(conf4)
	if err != nil {
		fmt.Fprintf(os.Stderr, "New DHT error: %v", err)
		os.Exit(1)
	}
	var d6 *dht.DHT
	if len(*ipv6Address) > 1 {
		fmt.Printf("Tring to bind to IPv6=%s\n", *ipv6Address)
		d6, err = dht.New(conf6)
		if err != nil {
			fmt.Fprintf(os.Stderr, "New DHT error: %v", err)
			os.Exit(1)
		}
		if err = d6.Start(); err != nil {
			fmt.Fprintf(os.Stderr, "DHT start error: %v", err)
			os.Exit(1)
		}
		go drainresults(d6)
	} else {
		fmt.Fprintf(os.Stderr, "Not binding to IPv6 interface.  If desired pass -v6=[address] for the\n")
		fmt.Fprintf(os.Stderr, "address you want the DHT to bind to.  Privacy addresses are not recommended\n")
		fmt.Fprintf(os.Stderr, "Since they can expire and connections will fail\n\n")
	}

	// For debugging.
	go http.ListenAndServe(fmt.Sprintf(":%d", httpPortTCP), nil)

	if err = d4.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "DHT start error: %v", err)
		os.Exit(1)
	}
	go drainresults(d4)
	for {
		d4.PeersRequest(string(ih), true)
		if len(*ipv6Address) > 1 {
			d6.PeersRequest(string(ih), true)
		}
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
