// Runs a node on UDP port 11221 and with a web server for debugging available
// at http://localhost:8080.
package main

import (
	"fmt"
	"time"

	l4g "code.google.com/p/log4go"
	"github.com/nictuku/dht"
	"net/http"
)

const (
	httpPort = 8080
)

func main() {
	l4g.AddFilter("stdout", l4g.DEBUG, l4g.NewConsoleLogWriter())

	d, err := dht.NewDHTNode(11221, 100, false)
	if err != nil {
		fmt.Println(err)
		return
	}
	go d.DoDHT()

	// Give the DHT some time to "warm-up" its routing table.
	time.Sleep(5 * time.Second)

	d.PeersRequest("\xd1\xc5\x67\x6a\xe7\xac\x98\xe8\xb1\x9f\x63\x56\x59\x05\x10\x5e\x3c\x4c\x37\xa2", false)

	go func() {
		for _ = range <-d.PeersRequestResults {

		}
	}()
	http.ListenAndServe(fmt.Sprintf(":%d", httpPort), nil)
}
