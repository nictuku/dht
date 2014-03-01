// Runs a node on UDP port 11221 that attempts to collect 100 peers for an
// infohash, then keeps running as a passive DHT node.
//
// IMPORTANT: if the UDP port is not reachable from the public internet, you
// may see very few results.
//
// To collect 100 peers, it usually has to contact some 10k nodes. This process
// is not instant and should take a minute or two, depending on your network
// connection.
//
//
// There is a builtin web server that can be used to collect debugging stats
// from http://localhost:8711/debug/vars.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"net/http"
	l4g "code.google.com/p/log4go"
	"github.com/nictuku/dht"
)

const (
	httpPortTCP = 8711
)

func main() {
	flag.Parse()
	// Change to l4g.DEBUG to see *lots* of debugging information.
	l4g.AddFilter("stdout", l4g.WARNING, l4g.NewConsoleLogWriter())
	if len(flag.Args()) != 1 {
		fmt.Fprintf(os.Stderr, "Usage: %v <infohash>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example infohash: d1c5676ae7ac98e8b19f63565905105e3c4c37a2\n")
		flag.PrintDefaults()
		os.Exit(1)
	}
	ih, err := dht.DecodeInfoHash(flag.Args()[0])
	if err != nil {
		l4g.Critical("DecodeInfoHash error: %v\n", err)
		os.Exit(1)
	}
	// Starts a DHT node with the default options. It picks a random UDP port. To change this, see dht.NewConfig.
	d, err := dht.New(nil)
	if err != nil {
		l4g.Critical("New DHT error: %v", err)
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
	fmt.Println("=========================== DHT")
	l4g.Warn("Note that there are many bad nodes that reply to anything you ask.")
	l4g.Warn("Peers found:")
	for r := range n.PeersRequestResults {
		for _, peers := range r {
			for _, x := range peers {
				l4g.Warn("%v", dht.DecodePeerAddress(x))
			}
		}
	}
}
