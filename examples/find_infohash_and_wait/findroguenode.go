// Runs a node on a random UDP port that attempts to collect peers for random
// generated infohashes, to find nodes that resonde to just any get_peers query.
//
// IMPORTANT: if the UDP port is not reachable from the public internet, you
// may see very few results.
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

	"github.com/btkador/dht"

        "crypto/rand"
        "encoding/hex"
)

const (
	httpPortTCP = 8711
)

func main() {
	flag.Parse()
	// Starts a DHT node with the default options. It picks a random UDP port. To change this, see dht.NewConfig.
	d, err := dht.New(&dht.Config{
                Address:                 "",
                Port:                    0, // Picks a random port.
                NumTargetPeers:          5,
                DHTRouters:              "router.magnets.im:6881,router.bittorrent.com:6881,dht.transmissionbt.com:6881",
                MaxNodes:                500,
                CleanupPeriod:           15 * time.Minute,
                SaveRoutingTable:        true,
                SavePeriod:              5 * time.Minute,
                RateLimit:               -1,
                MaxInfoHashes:           2048,
                MaxInfoHashPeers:        256,
                ClientPerMinuteLimit:    50,
                ThrottlerTrackedClients: 1000,
                UDPProto:                "udp4",
                MaxSearchQueries:        -1,
		MaxNodeDownloads:        1,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "New DHT error: %v", err)
		os.Exit(1)

	}
	// For debugging.
	go http.ListenAndServe(fmt.Sprintf(":%d", httpPortTCP), nil)

	if err = d.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "DHT start error: %v", err)
		os.Exit(1)
	}
	go drainresults(d)

	for {
		ih, err := dht.DecodeInfoHash(randomHex(20))
		if err != nil {
			fmt.Fprintf(os.Stderr, "DecodeInfoHash error: %v\n", err)
			os.Exit(1)
		}
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
			}
		}
	}
}


func randomHex(n int) string {
  bytes := make([]byte, n)
  if _, err := rand.Read(bytes); err != nil {
    return ""
  }
  return hex.EncodeToString(bytes)
}
