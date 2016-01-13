package dht

import (
	"crypto/rand"
	"net"
	"testing"
)

const (
	id = "01abcdefghij01234567"
)

type test struct {
	id        string
	rid       string
	proximity int
}

var table = []test{
	{id, id, 160},
	{id, "01abcdefghij01234566", 159},
	{id, "01abcdefghij01234568", 156},
	{id, "01abcdefghij01234569", 156},
	{id, "01abcdefghij0123456a", 153},
	{id, "01abcdefghij0123456b", 153},
	{id, "01abcdefghij0123456c", 153},
	{id, "01abcdefghij0123456d", 153},
	// Broken. I also don't know what is the correct number of common bits.
	// {"43b24884c97bdaa311ce020a2afc82d433f2553d", "dda6bef5317da6487d51b2a160ac349aef9c7cd3", 111},
}

func TestCommonBits(t *testing.T) {
	for _, v := range table {
		c := commonBits(v.id, v.rid)
		if c != v.proximity {
			t.Errorf("test failed for %v, wanted %d got %d", v.rid, v.proximity, c)
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
		r.neighborhoodUpkeep(genremoteNode(string(n)), "udp", newPeerStore(0, 0))
	}

	// Current state: 8 neighbors with low proximity.

	// Adds 7 neighbors from the static table. They should replace the
	// random ones, except for one.
	for _, v := range table[1:8] {
		r.neighborhoodUpkeep(genremoteNode(v.rid), "udp", newPeerStore(0, 0))
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
	r.kill(r.boundaryNode, newPeerStore(0, 0))

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

func genremoteNode(id string) *remoteNode {
	return &remoteNode{
		id:      id,
		address: randUDPAddr(),
	}

}

func randUDPAddr() net.UDPAddr {
	b := make([]byte, 4)
	for {
		n, err := rand.Read(b)
		if n != len(b) || err != nil {
			continue
		}
		break
	}
	return net.UDPAddr{
		IP:   b,
		Port: 1111,
	}
}
