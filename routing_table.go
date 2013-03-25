package dht

import (
	"expvar"
	"fmt"
	"net"
	"time"

	l4g "code.google.com/p/log4go"
)

func newRoutingTable() *routingTable {
	return &routingTable{
		&nTree{},
		make(map[string]*remoteNode),
		"",
		nil,
		0,
	}
}

type routingTable struct {
	*nTree
	// addresses is a map of UDP addresses in host:port format and
	// remoteNodes. A string is used because it's not possible to create
	// a map using net.UDPAddr
	// as a key.
	addresses map[string]*remoteNode

	// Neighborhood.
	nodeId       string // This shouldn't be here. Move neighborhood upkeep one level up?
	boundaryNode *remoteNode
	// How many prefix bits are shared between boundaryNode and nodeId.
	proximity int
}

// hostPortToNode finds or creates a node based on the specified hostPort
// specification, which should be a UDP address in the form "host:port".
// Specifying an illegal string is illegal and will generate a panic.
func (r *routingTable) hostPortToNode(hostPort string) (node *remoteNode, addr string, existed bool, err error) {
	if hostPort == "" {
		panic("programming error: hostPortToNode received a nil hostPort")
	}
	address, err := net.ResolveUDPAddr("udp", hostPort)
	if err != nil {
		return nil, "", false, err
	}
	if address.String() == "" {
		panic("programming error: address resolution for hostPortToNode returned an empty string")
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
		if addr == "" {
			l4g.Warn("reachableNodes: found empty address for node %x.", r.id)
			continue
		}
		if r.reachable && len(r.id) == 20 {
			tbl[addr] = []byte(r.id)
		}
	}
	return

}

func isValidAddr(addr string) bool {
	if addr == "" {
		return false
	}
	if h, p, err := net.SplitHostPort(addr); h == "" || p == "" || err != nil {
		return false
	}
	return true
}

// update the existing routingTable entry for this node by setting its correct
// infohash id. Gives an error if the node was not found.
func (r *routingTable) update(node *remoteNode) error {
	_, addr, existed, err := r.hostPortToNode(node.address.String())
	if err != nil {
		return err
	}
	if !isValidAddr(addr) {
		return fmt.Errorf("routingTable.update received an invalid address %v", addr)
	}
	if !existed {
		return fmt.Errorf("node missing from the routing table:", node.address.String())
	}
	if node.id != "" {
		r.nTree.insert(node)
		totalNodes.Add(1)
		r.addresses[addr].id = node.id
	}
	return nil
}

// insert the provided node into the routing table. Gives an error if another
// node already existed with that address.
func (r *routingTable) insert(node *remoteNode) error {
	if node.address == nil {
		panic("routingTable.insert() got a node with a nil address")
	}
	_, addr, existed, err := r.hostPortToNode(node.address.String())
	if err != nil {
		return err
	}
	if !isValidAddr(addr) {
		return fmt.Errorf("routingTable.insert received an invalid address %v", addr)

	}
	if existed {
		return nil // fmt.Errorf("node already existed in routing table: %v", node.address.String())
	}
	r.addresses[addr] = node
	// We don't know the ID of all nodes.
	if !bogusId(node.id) {
		// recursive version of node insertion.
		r.nTree.insert(node)
		totalNodes.Add(1)
	}
	return nil
}

// getOrCreateNode returns a node for hostPort, which can be an IP:port or
// Host:port, which will be resolved if possible.  Preferably return an entry
// that is already in the routing table, but create a new one otherwise, thus
// being idempotent.
func (r *routingTable) getOrCreateNode(id string, hostPort string) (node *remoteNode, err error) {
	node, addr, existed, err := r.hostPortToNode(hostPort)
	if err != nil {
		return nil, err
	}
	if existed {
		return node, nil
	}
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	node = newRemoteNode(udpAddr, id)
	return node, r.insert(node)
}

func (r *routingTable) kill(n *remoteNode) {
	delete(r.addresses, n.address.String())
	r.nTree.cut(InfoHash(n.id), 0)
	totalKilledNodes.Add(1)

	if n.id == r.boundaryNode.id {
		r.resetNeighborhoodBoundary()
	}
}

func (r *routingTable) resetNeighborhoodBoundary() {
	r.proximity = 0
	// Try to find a distant one within the neighborhood and promote it as
	// the most distant node in the neighborhood.
	neighbors := r.lookup(InfoHash(r.nodeId))
	if len(neighbors) > 0 {
		r.boundaryNode = neighbors[len(neighbors)-1]
		r.proximity = commonBits(r.nodeId, r.boundaryNode.id)
	}

}

func (r *routingTable) cleanup() (needPing []*remoteNode) {
	needPing = make([]*remoteNode, 0, 10)
	t0 := time.Now()
	// Needs some serious optimization.
	for addr, n := range r.addresses {
		if addr != n.address.String() {
			l4g.Warn("cleanup: node address mismatches: %v != %v. Deleting node", addr, n.address.String())
			r.kill(n)
			continue
		}
		if addr == "" {
			l4g.Warn("cleanup: found empty address for node %x. Deleting node", n.id)
			r.kill(n)
			continue
		}
		if n.reachable {
			if len(n.pendingQueries) == 0 {
				goto PING
			}
			// Tolerate 2 cleanup cycles.
			if time.Since(n.lastResponseTime) > cleanupPeriod*2+(time.Minute) {
				l4g.Trace("DHT: Old node seen %v ago. Deleting", time.Since(n.lastResponseTime))
				r.kill(n)
				continue
			}
			if time.Since(n.lastResponseTime).Nanoseconds() < cleanupPeriod.Nanoseconds()/2 {
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
		needPing = append(needPing, n)
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
func (r *routingTable) neighborhoodUpkeep(n *remoteNode) {
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

func (r *routingTable) addNewNeighbor(n *remoteNode, displaceBoundary bool) {
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

// pingSlowly pings the remote nodes in needPing, distributing the pings
// throughout an interval of cleanupPeriod, to avoid network traffic bursts. It
// doesn't really send the pings, but signals to the main goroutine that it
// should ping the nodes, using the pingRequest channel.
func pingSlowly(pingRequest chan *remoteNode, needPing []*remoteNode, cleanupPeriod time.Duration) {
	if len(needPing) == 0 {
		return
	}
	duration := cleanupPeriod - (1 * time.Minute)
	perPingWait := duration / time.Duration(len(needPing))
	for _, r := range needPing {
		pingRequest <- r
		<-time.After(perPingWait)
	}
}

var (
	totalKilledNodes = expvar.NewInt("totalKilledNodes")
	totalNodes       = expvar.NewInt("totalNodes")
)
