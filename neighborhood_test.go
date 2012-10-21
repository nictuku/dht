package dht

import (
	"testing"
)

var id = "01abcdefghij01234567"

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
	for i := 0; i < 5; i++ {
		// Add a few random nodes. I'm hoping none of the random
		// generated ones are closer than the ones in the test.
		r.neighborhoodUpkeep(&DHTRemoteNode{id: string(newNodeId())})
	}
	// Adds 7 neighbors from the static table. They should replace the
	// random ones, except for one.
	for _, v := range table[1:] {
		r.neighborhoodUpkeep(&DHTRemoteNode{id: v.id})
	}

	// Current state: 7 close neighbors, one random dude.
	// t.Logf("Neighbors:")
	// for _, v := range r.lookup(id) {
	// 	t.Logf("... %q", v.id)
	// }

	p := table[len(table)-1].proximity
	if r.proximity != p {
		t.Errorf("proximity wanted == %d, got %d", p, r.proximity)
		t.Logf("Neighbors:")
		for _, v := range r.lookup(id) {
			t.Logf("... %q", v.id)
		}
	}
	// Now let's kill the boundary neighbor.
	r.kill(r.distantNode)

	// The resulting boundary neighbor should now be one from the static table:
	p = table[len(table)-1].proximity
	if r.proximity < p {
		t.Errorf("proximity wanted >= %d, got %d", p, r.proximity)
		t.Logf("Later Neighbors:")
		for _, v := range r.lookup(id) {
			t.Logf("... %q", v.id)
		}
	}
}
