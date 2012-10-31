package dht

import (
	"crypto/rand"
	"net"
	"testing"

	l4g "code.google.com/p/log4go"
)

func init() {
	l4g.Global.AddFilter("stdout", l4g.DEBUG, l4g.NewConsoleLogWriter())
}

const (
	id = "01abcdefghij01234567"
)

type test struct {
	id        string
	proximity int
}

var table = []test{
	{id, 160},
	{"01abcdefghij01234566", 159},
	{"01abcdefghij01234568", 156},
	{"01abcdefghij01234569", 156},
	{"01abcdefghij0123456a", 153},
	{"01abcdefghij0123456b", 153},
	{"01abcdefghij0123456c", 153},
	{"01abcdefghij0123456d", 153},
}

func TestCommonBits(t *testing.T) {
	for _, v := range table {
		c := commonBits(id, v.id)
		if c != v.proximity {
			t.Errorf("test failed for %v, wanted %d got %d", v.id, v.proximity, c)
		}
	}
}

func TestUpkeep(t *testing.T) {
	r := newRoutingTable()
	r.nodeId = id

	// Current state: 0 neighbors.

	for i := 0; i < kNodes; i++ {
		// Add a few random nodes. They become neighbors and get added to the
		// routing table, but when they are displaced by closer nodes, they
		// are killed from the neighbors list and from the routing table, so
		// there should be no sign of them later on.
		n := randNodeId()
		n[0] = byte(0x3d) // Ensure long distance.
		r.neighborhoodUpkeep(genDHTRemoteNode(string(n)))
	}

	// Current state: 8 neighbors with low proximity.

	// Adds 7 neighbors from the static table. They should replace the
	// random ones, except for one.
	for _, v := range table[1:8] {
		r.neighborhoodUpkeep(genDHTRemoteNode(v.id))
	}

	// Current state: 7 close neighbors, one distant dude.

	// The proximity should be from the one remaining random node, thus very low.
	p := table[len(table)-1].proximity
	if r.proximity >= p {
		t.Errorf("proximity: %d >= %d: false", r.proximity, p)
		t.Logf("Neighbors:")
		for _, v := range r.lookup(id) {
			t.Logf("... %q", v.id)
		}
	}

	// Now let's kill the boundary nodes. Killing one makes the next
	// "random" node to become the next boundary node (they were kept in
	// the routing table). Repeat until all of them are removed.
	if r.boundaryNode == nil {
		t.Fatalf("tried to kill nil boundary node")
	}
	r.kill(r.boundaryNode)

	// The resulting boundary neighbor should now be one from the static
	// table, with high proximity.
	p = table[len(table)-1].proximity
	if r.proximity != p {
		t.Errorf("proximity wanted >= %d, got %d", p, r.proximity)
		t.Logf("Later Neighbors:")
		for _, v := range r.lookup(id) {
			t.Logf("... %x", v.id)
		}
	}
}

func genDHTRemoteNode(id string) *DHTRemoteNode {
	return &DHTRemoteNode{
		id:      id,
		address: randUDPAddr(),
	}

}

func randUDPAddr() *net.UDPAddr {
	b := make([]byte, 4)
	for {
		n, err := rand.Read(b)
		if n != len(b) || err != nil {
			continue
		}
		break
	}
	return &net.UDPAddr{
		IP:   b,
		Port: 1111,
	}
}
