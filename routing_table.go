package dht

import (
	"expvar"
	"fmt"
	"net"
	"time"

	log "github.com/golang/glog"
	"github.com/nictuku/nettools"
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

// hostPortToNode finds a node based on the specified hostPort specification,
// which should be a UDP address in the form "host:port".
func (r *routingTable) hostPortToNode(hostPort string, port string) (node *remoteNode, addr string, existed bool, err error) {
	if hostPort == "" {
		panic("programming error: hostPortToNode received a nil hostPort")
	}
	address, err := net.ResolveUDPAddr(port, hostPort)
	if err != nil {
		return nil, "", false, err
	}
	if address.String() == "" {
		return nil, "", false, fmt.Errorf("programming error: address resolution for hostPortToNode returned an empty string")
	}
	n, existed := r.addresses[address.String()]
	if existed && n == nil {
		return nil, "", false, fmt.Errorf("programming error: hostPortToNode found nil node in address table")
	}
	return n, address.String(), existed, nil
}

func (r *routingTable) length() int {
	return len(r.addresses)
}

func (r *routingTable) reachableNodes() (tbl map[string][]byte) {
	tbl = make(map[string][]byte)
	for addr, r := range r.addresses {
		if addr == "" {
			log.V(3).Infof("reachableNodes: found empty address for node %x.", r.id)
			continue
		}
		if r.reachable && len(r.id) == 20 {
			tbl[addr] = []byte(r.id)
		}
	}

	hexId := fmt.Sprintf("%x", r.nodeId)
	// This creates a new expvar everytime, but the alternative is too
	// bothersome (get the current value, type cast it, ensure it
	// exists..). Also I'm not using NewInt because I don't want to publish
	// the value.
	v := new(expvar.Int)
	v.Set(int64(len(tbl)))
	reachableNodes.Set(hexId, v)
	return

}

func (r *routingTable) numNodes() int {
	return len(r.addresses)
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
func (r *routingTable) update(node *remoteNode, proto string) error {
	_, addr, existed, err := r.hostPortToNode(node.address.String(), proto)
	if err != nil {
		return err
	}
	if !isValidAddr(addr) {
		return fmt.Errorf("routingTable.update received an invalid address %v", addr)
	}
	if !existed {
		return fmt.Errorf("node missing from the routing table: %v", node.address.String())
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
func (r *routingTable) insert(node *remoteNode, proto string) error {
	if node.address.Port == 0 {
		return fmt.Errorf("routingTable.insert() got a node with Port=0")
	}
	if node.address.IP.IsUnspecified() {
		return fmt.Errorf("routingTable.insert() got a node with a non-specified IP address")
	}
	_, addr, existed, err := r.hostPortToNode(node.address.String(), proto)
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
func (r *routingTable) getOrCreateNode(id string, hostPort string, proto string) (node *remoteNode, err error) {
	node, addr, existed, err := r.hostPortToNode(hostPort, proto)
	if err != nil {
		return nil, err
	}
	if existed {
		return node, nil
	}
	udpAddr, err := net.ResolveUDPAddr(proto, addr)
	if err != nil {
		return nil, err
	}
	node = newRemoteNode(*udpAddr, id)
	return node, r.insert(node, proto)
}

func (r *routingTable) kill(n *remoteNode, p *peerStore) {
	delete(r.addresses, n.address.String())
	r.nTree.cut(InfoHash(n.id), 0)
	totalKilledNodes.Add(1)

	if r.boundaryNode != nil && n.id == r.boundaryNode.id {
		r.resetNeighborhoodBoundary()
	}
	p.killContact(nettools.BinaryToDottedPort(n.addressBinaryFormat))
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

func (r *routingTable) cleanup(cleanupPeriod time.Duration, p *peerStore) (needPing []*remoteNode) {
	needPing = make([]*remoteNode, 0, 10)
	t0 := time.Now()
	// Needs some serious optimization.
	for addr, n := range r.addresses {
		if addr != n.address.String() {
			log.V(3).Infof("cleanup: node address mismatches: %v != %v. Deleting node", addr, n.address.String())
			r.kill(n, p)
			continue
		}
		if addr == "" {
			log.V(3).Infof("cleanup: found empty address for node %x. Deleting node", n.id)
			r.kill(n, p)
			continue
		}
		if n.reachable {
			if len(n.pendingQueries) == 0 {
				goto PING
			}
			// Tolerate 2 cleanup cycles.
			if time.Since(n.lastResponseTime) > cleanupPeriod*2+(cleanupPeriod/15) {
				log.V(4).Infof("DHT: Old node seen %v ago. Deleting", time.Since(n.lastResponseTime))
				r.kill(n, p)
				continue
			}
			if time.Since(n.lastResponseTime).Nanoseconds() < cleanupPeriod.Nanoseconds()/2 {
				// Seen recently. Don't need to ping.
				continue
			}

		} else {
			// Not reachable.
			if len(n.pendingQueries) > maxNodePendingQueries {
				// Didn't reply to 2 consecutive queries.
				log.V(4).Infof("DHT: Node never replied to ping. Deleting. %v", n.address)
				r.kill(n, p)
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
	log.V(3).Info("DHT: Routing table cleanup took %v", duration)
	return needPing
}

// neighborhoodUpkeep will update the routingtable if the node n is closer than
// the 8 nodes in our neighborhood, by replacing the least close one
// (boundary). n.id is assumed to have length 20.
func (r *routingTable) neighborhoodUpkeep(n *remoteNode, proto string, p *peerStore) {
	if r.boundaryNode == nil {
		r.addNewNeighbor(n, false, proto, p)
		return
	}
	if r.length() < kNodes {
		r.addNewNeighbor(n, false, proto, p)
		return
	}
	cmp := commonBits(r.nodeId, n.id)
	if cmp == 0 {
		// Not significantly better.
		return
	}
	if cmp > r.proximity {
		r.addNewNeighbor(n, true, proto, p)
		return
	}
}

func (r *routingTable) addNewNeighbor(n *remoteNode, displaceBoundary bool, proto string, p *peerStore) {
	if err := r.insert(n, proto); err != nil {
		log.V(3).Infof("addNewNeighbor error: %v", err)
		return
	}
	if displaceBoundary && r.boundaryNode != nil {
		// This will also take care of setting a new boundary.
		r.kill(r.boundaryNode, p)
	} else {
		r.resetNeighborhoodBoundary()
	}
	log.V(4).Infof("New neighbor added %s with proximity %d", nettools.BinaryToDottedPort(n.addressBinaryFormat), r.proximity)
}

// pingSlowly pings the remote nodes in needPing, distributing the pings
// throughout an interval of cleanupPeriod, to avoid network traffic bursts. It
// doesn't really send the pings, but signals to the main goroutine that it
// should ping the nodes, using the pingRequest channel.
func pingSlowly(pingRequest chan *remoteNode, needPing []*remoteNode, cleanupPeriod time.Duration, stop chan bool) {
	if len(needPing) == 0 {
		return
	}
	duration := cleanupPeriod - (1 * time.Minute)
	perPingWait := duration / time.Duration(len(needPing))
	for _, r := range needPing {
		pingRequest <- r
		select {
		case <-time.After(perPingWait):
		case <-stop:
			return
		}
	}
}

var (
	// totalKilledNodes is a monotonically increasing counter of times nodes were killed from
	// the routing table. If a node is later added to the routing table and killed again, it is
	// counted twice.
	totalKilledNodes = expvar.NewInt("totalKilledNodes")
	// totalNodes is a monotonically increasing counter of times nodes were added to the routing
	// table. If a node is removed then later added again, it is counted twice.
	totalNodes = expvar.NewInt("totalNodes")
	// reachableNodes is the count of all reachable nodes from a particular DHT node. The map
	// key is the local node's infohash. The value is a gauge with the count of reachable nodes
	// at the latest time the routing table was persisted on disk.
	reachableNodes = expvar.NewMap("reachableNodes")
)
