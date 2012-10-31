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

	// Neighborhood.
	nodeId       string // This shouldn't be here. Move neighborhood upkeep one level up?
	boundaryNode *DHTRemoteNode
	// How many prefix bits are shared between boundaryNode and nodeId.
	proximity int
}

func (r *routingTable) hostPortToNode(hostPort string) (node *DHTRemoteNode, addr string, existed bool, err error) {
	address, err := net.ResolveUDPAddr("udp", hostPort)
	if err != nil {
		return nil, "", false, err
	}
	n, existed := r.addresses[address.String()]
	return n, address.String(), existed, nil
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
	_, addr, existed, err := r.hostPortToNode(node.address.String())
	if err != nil {
		return err
	}
	if !existed {
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
	if node.address == nil {
		panic("routingTable.insert() got a node with a nil address")
	}
	_, addr, existed, err := r.hostPortToNode(node.address.String())
	if err != nil {
		return err
	}
	if existed {
		return nil // fmt.Errorf("node already existed in routing table: %v", node.address.String())
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

// getOrCreateNode returns a node for hostPort, which can be an IP:port or
// Host:port, which will be resolved if possible.  Preferably return an entry
// that is already in the routing table, but create a new one otherwise, thus
// being idempotent. updateRoutingTable should be set to true when the target
// host is _potentially_ useful and should be used in future queries and be
// provided to other hosts. In some cases, that should be true even before a
// node is proved to be reachable - the routing table should have mostly
// reachable nodes, but it doesn't hurt to have a few that aren't yet verified
// to be healthy.
func (r *routingTable) getOrCreateNode(id string, hostPort string, updateRoutingTable bool) (node *DHTRemoteNode, err error) {
	node, addr, existed, err := r.hostPortToNode(hostPort)
	if err != nil {
		return nil, err
	}
	if existed {
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
	if updateRoutingTable && id != "" {
		return node, r.insert(node)
	}
	return node, nil
}

func (r *routingTable) kill(n *DHTRemoteNode) {
	delete(r.addresses, n.address.String())
	r.nTree.cut(n.id, 0)
	totalKilledNodes.Add(1)

	if n.id == r.boundaryNode.id {
		r.resetNeighborhoodBoundary()
	}
}

func (r *routingTable) resetNeighborhoodBoundary() {
	r.proximity = 0
	// Try to find a distant one within the neighborhood and promote it as
	// the most distant node in the neighborhood.
	neighbors := r.lookup(r.nodeId)
	if len(neighbors) > 0 {
		r.boundaryNode = neighbors[len(neighbors)-1]
		r.proximity = commonBits(r.nodeId, r.boundaryNode.id)
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
	if r.proximity == 0 {
		r.addNewNeighbor(n, true)
		return
	}
	if r.length() < kNodes {
		r.addNewNeighbor(n, false)
		return
	}
	cmp := commonBits(r.nodeId, n.id)
	if cmp == 0 {
		// Same node id.
		return
	}
	if cmp > r.proximity {
		r.addNewNeighbor(n, true)
		return
	}
}

func (r *routingTable) addNewNeighbor(n *DHTRemoteNode, displaceBoundary bool) {
	if err := r.insert(n); err != nil {
		l4g.Warn("addNewNeighbor: %v", err)
		return
	}
	if displaceBoundary && r.boundaryNode != nil {
		// This will also take care of setting a new boundary.
		r.kill(r.boundaryNode)
	} else {
		r.resetNeighborhoodBoundary()
	}
	l4g.Trace("New neighbor added to neighborhood with proximity %d", r.proximity)
}

var (
	totalKilledNodes = expvar.NewInt("totalKilledNodes")
	totalNodes       = expvar.NewInt("totalNodes")
)
