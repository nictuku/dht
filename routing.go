package dht

import (
	log "github.com/golang/glog"
)

// DHT routing using a binary tree and no buckets.
//
// Nodes have ids of 20-bytes. When looking up an infohash for itself or for a
// remote host, the nodes have to look in its routing table for the closest
// nodes and return them.
//
// The distance between a node and an infohash is the XOR of the respective
// strings. This means that 'sorting' nodes only makes sense with an infohash
// as the pivot. You can't pre-sort nodes in any meaningful way.
//
// Most bittorrent/kademlia DHT implementations use a mix of bit-by-bit
// comparison with the usage of buckets. That works very well. But I wanted to
// try something different, that doesn't use buckets. Buckets have a single id
// and one calculates the distance based on that, speeding up lookups.
//
// I decided to lay out the routing table in a binary tree instead, which is
// more intuitive. At the moment, the implementation is a real tree, not a
// free-list, but it's performing well.
//
// All nodes are inserted in the binary tree, with a fixed height of 160 (20
// bytes). To lookup an infohash, I do an inorder traversal using the infohash
// bit for each level.
//
// In most cases the lookup reaches the bottom of the tree without hitting the
// target infohash, since in the vast majority of the cases it's not in my
// routing table. Then I simply continue the in-order traversal (but then to
// the 'left') and return after I collect the 8 closest nodes.
//
// To speed things up, I keep the tree as short as possible. The path to each
// node is compressed and later uncompressed if a collision happens when
// inserting another node.
//
// I don't know how slow the overall algorithm is compared to a implementation
// that uses buckets, but for what is worth, the routing table lookups don't
// even show on the CPU profiling anymore.

type nTree struct {
	zero, one *nTree
	value     *remoteNode
}

const (
	// Each query returns up to this number of nodes.
	kNodes = 8
	// Consider a node stale if it has more than this number of oustanding
	// queries from us.
	maxNodePendingQueries = 5
)

// recursive version of node insertion.
func (n *nTree) insert(newNode *remoteNode) {
	n.put(newNode, 0)
}

func (n *nTree) branchOut(n1, n2 *remoteNode, i int) {
	// Since they are branching out it's guaranteed that no other nodes
	// exist below this branch currently, so just create the respective
	// nodes until their respective bits are different.
	chr := byte(n1.id[i/8])
	bitPos := byte(i % 8)
	bit := (chr << bitPos) & 128

	chr2 := byte(n2.id[i/8])
	bitPos2 := byte(i % 8)
	bit2 := (chr2 << bitPos2) & 128

	if bit != bit2 {
		n.put(n1, i)
		n.put(n2, i)
		return
	}

	// Identical bits.
	if bit != 0 {
		n.one = &nTree{}
		n.one.branchOut(n1, n2, i+1)
	} else {
		n.zero = &nTree{}
		n.zero.branchOut(n1, n2, i+1)
	}
}

func (n *nTree) put(newNode *remoteNode, i int) {
	if i >= len(newNode.id)*8 {
		// Replaces the existing value, if any.
		n.value = newNode
		return
	}

	if n.value != nil {
		if n.value.id == newNode.id {
			// Replace existing compressed value.
			n.value = newNode
			return
		}
		// Compression collision. Branch them out.
		old := n.value
		n.value = nil
		n.branchOut(newNode, old, i)
		return
	}

	chr := byte(newNode.id[i/8])
	bit := byte(i % 8)
	if (chr<<bit)&128 != 0 {
		if n.one == nil {
			n.one = &nTree{value: newNode}
			return
		}
		n.one.put(newNode, i+1)
	} else {
		if n.zero == nil {
			n.zero = &nTree{value: newNode}
			return
		}
		n.zero.put(newNode, i+1)
	}
}

func (n *nTree) lookup(id InfoHash) []*remoteNode {
	ret := make([]*remoteNode, 0, kNodes)
	if n == nil || id == "" {
		return nil
	}
	return n.traverse(id, 0, ret, false)
}

func (n *nTree) lookupFiltered(id InfoHash) []*remoteNode {
	ret := make([]*remoteNode, 0, kNodes)
	if n == nil || id == "" {
		return nil
	}
	return n.traverse(id, 0, ret, true)
}

func (n *nTree) traverse(id InfoHash, i int, ret []*remoteNode, filter bool) []*remoteNode {
	if n == nil {
		return ret
	}
	if n.value != nil {
		if !filter || n.isOK(id) {
			return append(ret, n.value)
		}
	}
	if i >= len(id)*8 {
		return ret
	}
	if len(ret) >= kNodes {
		return ret
	}

	chr := byte(id[i/8])
	bit := byte(i % 8)

	// This is not needed, but it's clearer.
	var left, right *nTree
	if (chr<<bit)&128 != 0 {
		left = n.one
		right = n.zero
	} else {
		left = n.zero
		right = n.one
	}

	ret = left.traverse(id, i+1, ret, filter)
	if len(ret) >= kNodes {
		return ret
	}
	return right.traverse(id, i+1, ret, filter)
}

// cut goes down the tree and deletes the children nodes if all their leaves
// became empty.
func (n *nTree) cut(id InfoHash, i int) (cutMe bool) {
	if n == nil {
		return true
	}
	if i >= len(id)*8 {
		return true
	}
	chr := byte(id[i/8])
	bit := byte(i % 8)

	if (chr<<bit)&128 != 0 {
		if n.one.cut(id, i+1) {
			n.one = nil
			if n.zero == nil {
				return true
			}
		}
	} else {
		if n.zero.cut(id, i+1) {
			n.zero = nil
			if n.one == nil {
				return true
			}
		}
	}

	return false
}

func (n *nTree) isOK(ih InfoHash) bool {
	if n.value == nil || n.value.id == "" {
		return false
	}
	r := n.value

	if len(r.pendingQueries) > maxNodePendingQueries {
		log.V(3).Infof("DHT: Skipping because there are too many queries pending for this dude.")
		log.V(3).Infof("DHT: This shouldn't happen because we should have stopped trying already. Might be a BUG.")
		return false
	}

	recent := r.wasContactedRecently(ih)
	if log.V(4) {
		log.Infof("wasContactedRecently for ih=%x in node %x@%v returned %v", ih, r.id, r.address, recent)
	}
	if recent {
		return false
	}
	return true
}

func commonBits(s1, s2 string) int {
	// copied from jch's dht.cc.
	id1, id2 := []byte(s1), []byte(s2)

	i := 0
	for ; i < 20; i++ {
		if id1[i] != id2[i] {
			break
		}
	}

	if i == 20 {
		return 160
	}

	xor := id1[i] ^ id2[i]

	j := 0
	for (xor & 0x80) == 0 {
		xor <<= 1
		j++
	}
	return 8*i + j
}

// Calculates the distance between two hashes. In DHT/Kademlia, "distance" is
// the XOR of the torrent infohash and the peer node ID.  This is slower than
// necessary. Should only be used for displaying friendly messages.
func hashDistance(id1 InfoHash, id2 InfoHash) (distance string) {
	d := make([]byte, len(id1))
	if len(id1) != len(id2) {
		return ""
	} else {
		for i := 0; i < len(id1); i++ {
			d[i] = id1[i] ^ id2[i]
		}
		return string(d)
	}
	return ""
}
