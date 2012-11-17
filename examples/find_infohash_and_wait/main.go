// Runs a node on UDP port 11221 that attempt to collect 100 peers for an 
// infohash, then keeps running as a passive DHT node.
//
// To collect 100 peers, it usually has to contact some 10k nodes. This process
// is not instant and should take a minute or two, depending on your network
// connection.
//
// NOTE: The node has full debugging level enabled so you'll see a lot of output
// on the screen. It also runs a web server that can be used to collect
// debugging stats from http://localhost:8080/debug/vars.
package main

import (
	"fmt"
	"time"

	l4g "code.google.com/p/log4go"
	"github.com/nictuku/dht"
	"net/http"
)

const (
	httpPortTCP = 8080
	dhtPortUDP  = 11221
)

func main() {
	l4g.AddFilter("stdout", l4g.DEBUG, l4g.NewConsoleLogWriter())

	d, err := dht.NewDHTNode(dhtPortUDP, 100, false)
	if err != nil {
		fmt.Println(err)
		return
	}
	go d.DoDHT()
	go drainresults(d)

	// Give the DHT some time to "warm-up" its routing table.
	time.Sleep(5 * time.Second)

	d.PeersRequest("\xd1\xc5\x67\x6a\xe7\xac\x98\xe8\xb1\x9f\x63\x56\x59\x05\x10\x5e\x3c\x4c\x37\xa2", false)

	http.ListenAndServe(fmt.Sprintf(":%d", httpPortTCP), nil)
}

// drainresults loops, constantly reading any new peer information sent by the
// DHT node and just ignoring them. We don't care about those :-P.
func drainresults(n *dht.DHT) {
	for {
		<-n.PeersRequestResults
	}
}
