package dht

import (
	"crypto/rand"
	"expvar"
	"fmt"
	"net"
	"time"

	l4g "code.google.com/p/log4go"
)

func newRoutingTable() *routingTable {
	return &routingTable{
		&nTree{},
		make(map[string]*DHTRemoteNode),
		"",
		nil,
		0,
	}
}

type routingTable struct {
	*nTree
	addresses map[string]*DHTRemoteNode

	// Neighborhood. Not very pretty. 

	nodeId      string // This shouldn't be here. Move neighborhood upkeep one level up?
	distantNode *DHTRemoteNode
	// how many prefix bits are shared between neighborhoodDistantNode and
	// nodeId.
	proximity int
}

func (r *routingTable) hostPortToNode(hostPort string) (node *DHTRemoteNode, addr string, ok bool) {
	address, err := net.ResolveUDPAddr("udp", hostPort)
	if err != nil {
		return nil, "", false
	}
	n, ok := r.addresses[address.String()]
	return n, address.String(), ok
}

func (r *routingTable) length() int {
	return len(r.addresses)
}

func (r *routingTable) reachableNodes() (tbl map[string][]byte) {
	tbl = make(map[string][]byte)
	for addr, r := range r.addresses {
		if r.reachable && len(r.id) == 20 {
			tbl[addr] = []byte(r.id)
		}
	}
	return

}

// update the existing routingTable entry for this node, giving an error if the
// node was not found.
func (r *routingTable) update(node *DHTRemoteNode) error {
	_, addr, ok := r.hostPortToNode(node.address.String())
	if !ok {
		return fmt.Errorf("node missing from the routing table:", node.address.String())
	}
	r.addresses[addr] = node
	if node.id != "" {
		r.nTree.insert(node)
		totalNodes.Add(1)
	}
	return nil
}

// insert the provided node into the routing table. Gives an error if another
// node already existed with that address.
func (r *routingTable) insert(node *DHTRemoteNode) error {
	_, addr, ok := r.hostPortToNode(node.address.String())
	if ok {
		return fmt.Errorf("node already existed in routing table:", node.address.String())
	}
	r.addresses[addr] = node
	// We don't know the ID of all nodes.
	if node.id != "" {
		// recursive version of node insertion.
		r.nTree.insert(node)
		totalNodes.Add(1)
	}
	return nil
}

// forceNode returns a node for hostPort, which can be an IP:port or Host:port,
// which will be resolved if possible.  Preferably return an entry that is
// already in the routing table, but create a new one otherwise, thus being idempotent.
func (r *routingTable) forceNode(id string, hostPort string) (node *DHTRemoteNode, err error) {
	node, addr, ok := r.hostPortToNode(hostPort)
	if ok {
		return node, nil
	}
	n, err := rand.Read(make([]byte, 1))
	if err != nil {
		return nil, err
	}
	udpaddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	node = &DHTRemoteNode{
		address:        udpaddr,
		lastQueryID:    n,
		id:             id,
		reachable:      false,
		pendingQueries: map[string]*queryType{},
		pastQueries:    map[string]*queryType{},
	}
	return node, r.insert(node)
}

func (r *routingTable) kill(n *DHTRemoteNode) {
	delete(r.addresses, n.address.String())
	r.nTree.cut(n.id, 0)
	totalKilledNodes.Add(1)

	if n.id == r.distantNode.id {
		r.resetNeighborhoodBoundary()
	}
}

func (r *routingTable) resetNeighborhoodBoundary() {
	r.proximity = 0
	// Try to find a distant one within the neighborhood and promote it as
	// the most distant node in the neighborhood.
	neighbors := r.lookup(r.nodeId)
	if len(neighbors) > 0 {
		r.distantNode = neighbors[len(neighbors)-1]
		r.proximity = commonBits(r.nodeId, r.distantNode.id)
	}

}

func (r *routingTable) cleanup() (needPing []string) {
	needPing = make([]string, 10)
	t0 := time.Now()
	// Needs some serious optimization.
	for _, n := range r.addresses {
		if n.reachable {
			if len(n.pendingQueries) == 0 {
				goto PING
			}
			if time.Since(n.lastTime) > cleanupPeriod*2 {
				l4g.Trace("DHT: Old dude seen %v ago. Deleting.", time.Since(n.lastTime))
				r.kill(n)
				continue
			}
			if time.Since(n.lastTime).Nanoseconds() < cleanupPeriod.Nanoseconds()/2 {
				// Seen recently. Don't need to ping.
				continue
			}

		} else {
			// Not reachable.
			if len(n.pendingQueries) > 2 {
				// Didn't reply to 2 consecutive queries.
				l4g.Trace("DHT: Node never replied to ping. Deleting. %v", n.address)
				r.kill(n)
				continue
			}
		}
	PING:
		needPing = append(needPing, n.address.String())
	}
	duration := time.Since(t0)
	// If this pauses the server for too long I may have to segment the cleanup.
	// 2000 nodes: it takes ~12ms
	// 4000 nodes: ~24ms.
	l4g.Info("DHT: Routing table cleanup took %v", duration)
	return needPing
}

// neighborhoodUpkeep will update the routingtable if the node n is closer than
// the 8 nodes in our neighborhood, by replacing the least close one (boundary).
func (r *routingTable) neighborhoodUpkeep(n *DHTRemoteNode) {
	cmp := commonBits(r.nodeId, n.id)
	if cmp == 0 {
		// Same node id.
		return
	}
	// Rewrite this stuff. Was debugging.
	closer := r.proximity == 0 || cmp > r.proximity
	if closer {
		r.insert(n)
		l4g.Info("New neighbor added to neighborhood with proximity %d", r.proximity)

		// The boundary node fell off the neighborhood. Kill it.
		if r.distantNode != nil {
			// This will also take care of setting a new boundary.
			r.kill(r.distantNode)
		} else {
			r.resetNeighborhoodBoundary()
		}
	} else if r.length() < kNodes {
		r.insert(n)
		r.resetNeighborhoodBoundary()
		l4g.Info("New neighbor added to neighborhood with proximity %d", r.proximity)
	}
}

func (r *routingTable) replaceLastNeighbor(n *DHTRemoteNode, proximity int) {
	r.distantNode = n
	r.proximity = proximity
}

var (
	totalKilledNodes = expvar.NewInt("totalKilledNodes")
	totalNodes       = expvar.NewInt("totalNodes")
)
