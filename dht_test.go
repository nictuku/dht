package dht

import (
	"expvar"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/nictuku/nettools"
)

var cfg = newTestConfig()

// ExampleDHT is a simple example that searches for a particular infohash and
// exits when it finds any peers. A stand-alone version can be found in the
// examples/ directory.
func ExampleDHT() {
	if testing.Short() {
		fmt.Println("Peer found for the requested infohash or the test was skipped")
		return
	}
	d, err := New(0, 100, cfg)
	if err != nil {
		fmt.Println(err)
		return
	}
	go d.Run()

	infoHash, err := DecodeInfoHash("d1c5676ae7ac98e8b19f63565905105e3c4c37a2")
	if err != nil {
		fmt.Printf("DecodeInfoHash faiure: %v", err)
		return
	}

	tick := time.Tick(time.Second)

	var infoHashPeers map[InfoHash][]string
M:
	for {
		select {
		case <-tick:
			// Repeat the request until a result appears, querying nodes that haven't been
			// consulted before and finding close-by candidates for the infohash.
			d.PeersRequest(string(infoHash), false)
		case infoHashPeers = <-d.PeersRequestResults:
			break M
		case <-time.After(30 * time.Second):
			fmt.Printf("Could not find new peers: timed out")
			return
		}
	}
	for ih, peers := range infoHashPeers {
		if len(peers) > 0 {
			// Peers are encoded in binary format. Decoding example using github.com/nictuku/nettools:
			// for _, peer := range peers {
			// 	fmt.Println(DecodePeerAddress(peer))
			// }

			if fmt.Sprintf("%x", ih) == "d1c5676ae7ac98e8b19f63565905105e3c4c37a2" {
				fmt.Println("Peer found for the requested infohash or the test was skipped")
				return
			}
		}
	}

	// Output:
	// Peer found for the requested infohash or the test was skipped
}

func startDHTNode(t *testing.T) *DHT {
	node, err := New(0, 100, cfg)
	node.nodeId = string(randNodeId())
	if err != nil {
		t.Errorf("New(): %v", err)
	}
	go node.Run()
	return node
}

// Requires Internet access and can be flaky if the server or the internet is
// slow.
func TestDHTLarge(t *testing.T) {
	if testing.Short() {
		t.Skip("TestDHTLarge requires internet access and can be flaky. Skipping in short mode.")
	}
	defer stats(t)
	node := startDHTNode(t)
	realDHTNodes := []string{
		"1.a.magnets.im",
		"router.utorrent.com",
	}
	for _, addr := range realDHTNodes {
		ip, err := net.LookupHost(addr)
		if err != nil {
			t.Error(err)
			continue
		}
		node.AddNode(ip[0] + ":6881")
	}

	// Test that we can reach at least one node.
	success := false
	var (
		reachable int
		v         expvar.Var
		err       error
	)
	for i := 0; i < 10; i++ {
		v = expvar.Get("totalNodesReached")
		reachable, err = strconv.Atoi(v.String())
		if err != nil {
			t.Errorf("totalNodesReached conversion to int failed: %v", err)
			continue
		}
		if reachable > 0 {
			t.Logf("Contacted %d DHT nodes.", reachable)
			success = true
			break
		}
		time.Sleep(time.Second)
	}
	if !success {
		t.Fatal("No external DHT node could be contacted.")
	}

	// Test that we can find peers for a known torrent in a timely fashion.
	//
	// Torrent from: http://www.clearbits.net/torrents/244-time-management-for-anarchists-1
	infoHash := InfoHash("\xb4\x62\xc0\xa8\xbc\xef\x1c\xe5\xbb\x56\xb9\xfd\xb8\xcf\x37\xff\xd0\x2f\x5f\x59")
	go node.PeersRequest(string(infoHash), true)
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(10 * time.Second)
		timeout <- true
	}()
	var infoHashPeers map[InfoHash][]string
	select {
	case infoHashPeers = <-node.PeersRequestResults:
		t.Logf("Found %d peers.", len(infoHashPeers[infoHash]))
	case <-timeout:
		t.Fatal("Could not find new peers: timed out")
	}
	for ih, peers := range infoHashPeers {
		if infoHash != ih {
			t.Fatal("Unexpected infohash returned")
		}
		if len(peers) == 0 {
			t.Fatal("Could not find new torrent peers.")
		}
		for _, peer := range peers {
			t.Logf("peer found: %v", nettools.BinaryToDottedPort(peer))
		}
	}
}

func TestNewDHTConfig(t *testing.T) {
	c := NewDefaultConfig()
	d, err := New(6060, 10, c)
	if err != nil {
		t.Fatalf("DHT failed to init with config: %v", err)
	}
	if d.config != c {
		t.Fatal("DHT not initialized with config")
	}
}

func TestRegisterFlags(t *testing.T) {
	c := &Config{
		DHTRouters:    "example.router.com:6060",
		MaxNodes:      2020,
		CleanupPeriod: time.Second,
		SavePeriod:    time.Second * 2,
		RateLimit:     999,
	}
	RegisterFlags(c)
	if flag.Lookup("routers").DefValue != c.DHTRouters {
		t.Fatal("Incorrect routers flag")
	}
	if flag.Lookup("maxNodes").DefValue != strconv.FormatInt(int64(c.MaxNodes), 10) {
		t.Fatal("Incorrect maxNodes flag")
	}
	if flag.Lookup("cleanupPeriod").DefValue != c.CleanupPeriod.String() {
		t.Fatal("Incorrect cleanupPeriod flag")
	}
	if flag.Lookup("savePeriod").DefValue != c.SavePeriod.String() {
		t.Fatal("Incorrect routers flag")
	}
	if flag.Lookup("rateLimit").DefValue != strconv.FormatInt(c.RateLimit, 10) {
		t.Fatal("Incorrect routers flag")
	}
}

func stats(t *testing.T) {
	t.Logf("=== Stats ===")
	t.Logf("totalNodesReached: %v", totalNodesReached)
	t.Logf("totalGetPeersDupes: %v", totalGetPeersDupes)
	t.Logf("totalFindNodeDupes: %v", totalFindNodeDupes)
	t.Logf("totalPeers: %v", totalPeers)
	t.Logf("totalSentFindNode: %v", totalSentFindNode)
	t.Logf("totalSentGetPeers: %v", totalSentGetPeers)
}

func init() {
	rand.Seed((time.Now().Unix() % (1e9 - 1)))
}
