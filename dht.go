// DHT node for Taipei Torrent, for tracker-less peer information exchange.
// Status:
//  Supports all DHT operations from the specification, except:
//  - TODO: handle announce_peer.

package dht

// Summary from the bittorrent DHT protocol specification:
//
// Message types:
//  - query
//  - response
//  - error
//
// RPCs:
//      ping:
//         see if node is reachable and save it on routing table.
//      find_node:
//	       run when DHT node count drops, or every X minutes. Just to ensure
//	       our DHT routing table is still useful.
//      get_peers:
//	       the real deal. Iteratively queries DHT nodes and find new sources
//	       for a particular infohash.
//	announce_peer:
//         announce that the peer associated with this node is downloading a
//         torrent.
//
// Reference:
//     http://www.bittorrent.org/beps/bep_0005.html
//

import (
	"crypto/rand"
	"expvar"
	"flag"
	"fmt"
	"net"
	"strings"
	"time"

	l4g "code.google.com/p/log4go"
	"github.com/nictuku/nettools"
)

var (
	dhtRouter        string
	maxNodes         int
	cleanupPeriod    time.Duration
	savePeriod       time.Duration
	rateLimit        int64
	rateLimitEnabled = true
)

func init() {
	// TODO: Run my own router.
	flag.StringVar(&dhtRouter, "dhtRouter", "router.utorrent.com:6881",
		"IP:Port address of the DHT router used to bootstrap the DHT network.")
	flag.IntVar(&maxNodes, "maxNodes", 1000,
		"Maximum number of nodes to store in the routing table, in memory.")
	flag.DurationVar(&cleanupPeriod, "cleanupPeriod", 15*time.Minute,
		"How often to ping nodes in the network to see if they are reachable.")
	flag.DurationVar(&savePeriod, "savePeriod", 5*time.Minute,
		"How often to save the routing table to disk.")
	flag.Int64Var(&rateLimit, "rateLimit", 100,
		"Maximum packets per second to be processed. Beyond this limit they are silently dropped. Set to -1 to disable rate limiting.")

	// TODO: Control the verbosity via flag.
	l4g.AddFilter("stdout", l4g.WARNING, l4g.NewConsoleLogWriter())
}

// DHT should be created by NewDHTNode(). It provides DHT features to a
// torrent client, such as finding new peers for torrent downloads without
// requiring a tracker.
type DHT struct {
	nodeId string
	port   int

	routingTable *routingTable
	peerStore    *peerStore

	numTargetPeers int
	conn           *net.UDPConn
	Logger         Logger

	exploredNeighborhood bool

	// Public channels:
	remoteNodeAcquaintance chan string
	peersRequest           chan peerReq
	PeersRequestResults    chan map[string][]string // key = infohash, v = slice of peers.
	clientThrottle         *nettools.ClientThrottle

	store *DHTStore
}

func NewDHTNode(port, numTargetPeers int, storeEnabled bool) (node *DHT, err error) {
	node = &DHT{
		port:                 port,
		routingTable:         newRoutingTable(),
		peerStore:            newPeerStore(),
		PeersRequestResults:  make(chan map[string][]string, 1),
		exploredNeighborhood: false,
		// Buffer to avoid blocking on sends.
		remoteNodeAcquaintance: make(chan string, 100),
		// Buffer to avoid deadlocks and blocking on sends.
		peersRequest: make(chan peerReq, 100),

		numTargetPeers: numTargetPeers,
		clientThrottle: nettools.NewThrottler(),
	}
	c := openStore(port, storeEnabled)
	node.store = c
	if len(c.Id) != 20 {
		c.Id = randNodeId()
		l4g.Info("newId: %x %d", c.Id, len(c.Id))
		saveStore(*c)
	}
	// The types don't match because JSON marshalling needs []byte.
	node.nodeId = string(c.Id)

	expNodeIds.Add(fmt.Sprintf("%x", node.nodeId), 0)

	// XXX refactor.
	node.routingTable.nodeId = node.nodeId

	// This is called before the engine is up and ready to read from the
	// underlying channel.
	go func() {
		for addr, _ := range c.Remotes {
			node.AddNode(addr)
		}
	}()
	return
}

// Logger allows the DHT client to attach hooks for certain RPCs so it can log
// interesting events any way it wants.
type Logger interface {
	GetPeers(*net.UDPAddr, string, string)
}

type peerReq struct {
	ih       string
	announce bool
}

// PeersRequest asks the DHT to search for more peers for the infoHash
// provided. announce should be true if the connected peer is actively
// downloading this infohash, which is normally the case - unless this DHT node
// is just a router that doesn't downloads torrents.
func (d *DHT) PeersRequest(ih string, announce bool) {
	d.peersRequest <- peerReq{ih, announce}
}

// AddNode informs the DHT of a new node it should add to its routing table.
// addr is a string containing the target node's "host:port" UDP address.
func (d *DHT) AddNode(addr string) {
	d.remoteNodeAcquaintance <- addr
}

// Asks for more peers for a torrent.
func (d *DHT) getPeers(infoHash string) {
	closest := d.routingTable.lookupFiltered(infoHash)
	for _, r := range closest {
		d.getPeersFrom(r, infoHash)
	}
}

// Asks for more peers for a torrent.
func (d *DHT) findNode(id string) {
	// Doesn't use lookupFiltered because we're interested in the closest
	// results, always.
	closest := d.routingTable.lookup(id)
	for _, r := range closest {
		skip := false
		for _, q := range r.pendingQueries {
			if q.Type == "find_node" && q.ih == id {
				skip = true
				break
			}
		}
		if !skip {
			d.findNodeFrom(r, id)
		}
	}
}

// DoDHT is the DHT node main loop and should be run as a goroutine by the torrent client.
func (d *DHT) DoDHT() {
	socketChan := make(chan packetType)
	socket, err := listen(d.port)
	if err != nil {
		return
	}
	d.conn = socket
	bytesArena := newArena(maxUDPPacketSize, 500)
	go readFromSocket(socket, socketChan, bytesArena)

	// Bootstrap the network.
	d.ping(dhtRouter)

	cleanupTicker := time.Tick(cleanupPeriod)

	saveTicker := make(<-chan time.Time)
	if d.store != nil {
		saveTicker = time.Tick(savePeriod)
	}

	var fillTokenBucket <-chan time.Time
	tokenBucket := rateLimit

	if rateLimit < 0 {
		rateLimitEnabled = false
		l4g.Warn("rate limiting disabled")
	} else {
		// Token bucket for limiting the number of packets per second.
		fillTokenBucket = time.Tick(time.Second / 10)
		if rateLimit < 10 {
			// Less than 10 leads to rounding problems.
			rateLimit = 10
		}
	}
	l4g.Info("DHT: Starting DHT node %x.", d.nodeId)

	for {
		select {
		case addr := <-d.remoteNodeAcquaintance:
			d.helloFromPeer(addr)
		case peersRequest := <-d.peersRequest:
			// torrent server is asking for more peers for a particular infoHash.  Ask the closest nodes for
			// directions. The goroutine will write into the PeersNeededResults channel.
			if peersRequest.announce {
				d.peerStore.addLocalDownload(peersRequest.ih)
			}

			if d.peerStore.count(peersRequest.ih) < d.numTargetPeers {
				l4g.Warn("DHT: torrent client asking more peers for %x. Calling getPeers().", peersRequest.ih)
				d.getPeers(peersRequest.ih)
			}

		case p := <-socketChan:
			totalRecv.Add(1)
			if rateLimitEnabled {
				if tokenBucket > 0 {
					d.processPacket(p)
					tokenBucket -= 1
				} else {
					// In the future it might be better to avoid dropping things like ping replies.
					totalDroppedPackets.Add(1)
				}
			} else {
				d.processPacket(p)
			}
			bytesArena.Push(p.b)

		case <-fillTokenBucket:
			if tokenBucket < rateLimit {
				tokenBucket += rateLimit / 10
			}
		case <-cleanupTicker:
			needPing := d.routingTable.cleanup()
			go func(needPing []*remoteNode) {
				// Don't ping all hosts at the same time -
				// spread them out.
				duration := cleanupPeriod - (1 * time.Minute)
				perPingWait := duration / time.Duration(len(needPing))
				for _, r := range needPing {
					d.pingNode(r)
					<-time.After(perPingWait)
				}
			}(needPing)
		case <-saveTicker:
			tbl := d.routingTable.reachableNodes()
			if len(tbl) > 5 {
				d.store.Remotes = tbl
				saveStore(*d.store)
			}
		}
	}
}

func (d *DHT) helloFromPeer(addr string) {
	// We've got a new node id. We need to:
	// - see if we know it already, skip accordingly.
	// - ping it and see if it's reachable.
	// - if it responds, save it in the routing table.
	_, addrResolved, existed, err := d.routingTable.hostPortToNode(addr)
	if err != nil {
		l4g.Warn("helloFromPeer error: %v", err)
		return
	}
	if existed {
		// Node host+port already known.
		return
	}
	if d.routingTable.length() < maxNodes {
		d.ping(addrResolved)
		return
	}
}

func (d *DHT) processPacket(p packetType) {
	if !d.clientThrottle.CheckBlock(p.raddr.IP.String()) {
		totalPacketsFromBlockedHosts.Add(1)
		return
	}
	if p.b[0] != 'd' {
		// Malformed DHT packet. There are protocol extensions out
		// there that we don't support or understand.
		return
	}
	r, err := readResponse(p)
	if err != nil {
		l4g.Warn("DHT: readResponse Error: %v, %q", err, string(p.b))
		return
	}
	switch {
	// Response.
	case r.Y == "r":
		node, addr, existed, err := d.routingTable.hostPortToNode(p.raddr.String())
		if err != nil {
			l4g.Info("DHT readResponse error processing response: %v", err)
			return
		}
		if !existed {
			l4g.Info("DHT: Received reply from a host we don't know: %v", p.raddr)
			if d.routingTable.length() < maxNodes {
				d.ping(addr)
			}
			return
		}
		// Fix the node ID.
		if node.id == "" {
			node.id = r.R.Id
			d.routingTable.update(node)
		}
		if node.id != r.R.Id {
			l4g.Debug("DHT: Node changed IDs %x => %x", node.id, r.R.Id)
		}
		if query, ok := node.pendingQueries[r.T]; ok {
			if !node.reachable {
				node.reachable = true
				totalReachableNodes.Add(1)
			}
			node.lastTime = time.Now()
			d.routingTable.neighborhoodUpkeep(node)

			// If this is the first host added to the routing table, attempt a
			// recursive lookup of our own address, to build our neighborhood.
			if !d.exploredNeighborhood {
				d.findNode(d.nodeId)
			}
			d.exploredNeighborhood = true

			switch query.Type {
			case "ping":
				// Served its purpose, nothing else to be done.
				l4g.Trace("DHT: Received ping reply")
				totalRecvPingReply.Add(1)
			case "get_peers":
				d.processGetPeerResults(node, r)
			case "find_node":
				d.processFindNodeResults(node, r)
			case "announce_peer":
				// Nothing to do. In the future, update counters.
			default:
				l4g.Info("DHT: Unknown query type: %v from %v", query.Type, addr)
			}
			node.pastQueries[r.T] = query
			delete(node.pendingQueries, r.T)
		} else {
			l4g.Info("DHT: Unknown query id: %v", r.T)
		}
	case r.Y == "q":
		_, addr, existed, err := d.routingTable.hostPortToNode(p.raddr.String())
		if err != nil {
			l4g.Warn("Error readResponse error processing query: %v", err)
			return
		}
		if !existed {
			// Another candidate for the routing table. See if it's reachable.
			if d.routingTable.length() < maxNodes {
				d.ping(addr)
			}
		}
		switch r.Q {
		case "ping":
			d.replyPing(p.raddr, r)
		case "get_peers":
			d.replyGetPeers(p.raddr, r)
		case "find_node":
			d.replyFindNode(p.raddr, r)
		case "announce_peer":
			d.replyAnnouncePeer(p.raddr, r)
		default:
			l4g.Trace("DHT: non-implemented handler for type %v", r.Q)
		}
	default:
		l4g.Info("DHT: Bogus DHT query from %v.", p.raddr)
	}
}

func (d *DHT) ping(address string) {
	r, err := d.routingTable.getOrCreateNode("", address)
	if err != nil {
		l4g.Info("ping error for address %v: %v", address, err)
		return
	}
	d.pingNode(r)
}

func (d *DHT) pingNode(r *remoteNode) {
	l4g.Debug("DHT: ping => %+v\n", r.address)
	t := r.newQuery("ping")

	queryArguments := map[string]interface{}{"id": d.nodeId}
	query := queryMessage{t, "q", "ping", queryArguments}
	sendMsg(d.conn, r.address, query)
	totalSentPing.Add(1)
}

func (d *DHT) getPeersFrom(r *remoteNode, ih string) {
	totalSentGetPeers.Add(1)
	ty := "get_peers"
	transId := r.newQuery(ty)
	r.pendingQueries[transId].ih = ih
	queryArguments := map[string]interface{}{
		"id":        d.nodeId,
		"info_hash": ih,
	}
	query := queryMessage{transId, "q", ty, queryArguments}
	l4g.Trace(func() string {
		x := hashDistance(r.id, ih)
		return fmt.Sprintf("DHT sending get_peers. nodeID: %x , InfoHash: %x , distance: %x", r.id, ih, x)
	})
	sendMsg(d.conn, r.address, query)
}

func (d *DHT) findNodeFrom(r *remoteNode, id string) {
	totalSentFindNode.Add(1)
	ty := "find_node"
	transId := r.newQuery(ty)
	r.pendingQueries[transId].ih = id
	queryArguments := map[string]interface{}{
		"id":     d.nodeId,
		"target": id,
	}
	query := queryMessage{transId, "q", ty, queryArguments}
	l4g.Trace(func() string {
		x := hashDistance(r.id, id)
		return fmt.Sprintf("DHT sending find_node. nodeID: %x , target ID: %x , distance: %x", r.id, id, x)
	})
	sendMsg(d.conn, r.address, query)
}

// announcePeer sends a message to the destination address to advertise that
// our node is a peer for this infohash, using the provided token to
// 'authenticate'.
func (d *DHT) announcePeer(address *net.UDPAddr, ih string, token string) {
	r, err := d.routingTable.getOrCreateNode("", address.String())
	if err != nil {
		l4g.Trace("announcePeer:", err)
		return
	}
	ty := "announce_peer"
	l4g.Trace("DHT: announce_peer => %v %x %x\n", address, ih, token)
	transId := r.newQuery(ty)
	queryArguments := map[string]interface{}{
		"id":        d.nodeId,
		"info_hash": ih,
		"port":      d.port,
		"token":     token,
	}
	query := queryMessage{transId, "q", ty, queryArguments}
	sendMsg(d.conn, address, query)
}

func (d *DHT) replyAnnouncePeer(addr *net.UDPAddr, r responseType) {
	l4g.Trace(func() string {
		return fmt.Sprintf("DHT: announce_peer. Host %v, nodeID: %x, infoHash: %x, peerPort %d, distance to me %x",
			addr, r.A.Id, r.A.InfoHash, r.A.Port, hashDistance(r.A.InfoHash, d.nodeId),
		)
	})
	peerAddr := net.TCPAddr{IP: addr.IP, Port: r.A.Port}
	d.peerStore.addContact(r.A.InfoHash, nettools.DottedPortToBinary(peerAddr.String()))
	// Always reply positively. jech says this is to avoid "back-tracking", not sure what that means.
	reply := replyMessage{
		T: r.T,
		Y: "r",
		R: map[string]interface{}{"id": d.nodeId},
	}
	sendMsg(d.conn, addr, reply)
}

func (d *DHT) replyGetPeers(addr *net.UDPAddr, r responseType) {
	totalRecvGetPeers.Add(1)
	l4g.Info(func() string {
		return fmt.Sprintf("DHT get_peers. Host: %v , nodeID: %x , InfoHash: %x , distance to me: %x", addr, r.A.Id, r.A.InfoHash, hashDistance(r.A.InfoHash, d.nodeId))
	})

	if d.Logger != nil {
		d.Logger.GetPeers(addr, r.A.Id, r.A.InfoHash)
	}

	ih := r.A.InfoHash
	// TODO(nictuku): Implement token rotation.
	// See https://github.com/jech/dht/blob/master/dht.c
	// rotate_secrets(), make_token()
	r0 := map[string]interface{}{"id": d.nodeId, "token": "blabla"}
	reply := replyMessage{
		T: r.T,
		Y: "r",
		R: r0,
	}

	if peerContacts := d.peersForInfoHash(ih); len(peerContacts) > 0 {
		reply.R["values"] = peerContacts
	} else {
		reply.R["nodes"] = d.nodesForInfoHash(ih)
	}
	sendMsg(d.conn, addr, reply)
}

func (d *DHT) nodesForInfoHash(ih string) string {
	n := make([]string, 0, kNodes)
	for _, r := range d.routingTable.lookupFiltered(ih) {
		n = append(n, r.id+nettools.DottedPortToBinary(r.address.String()))
	}
	l4g.Trace("replyGetPeers: Nodes only. Giving %d", len(n))
	return strings.Join(n, "")
}

func (d *DHT) peersForInfoHash(ih string) []string {
	peerContacts := d.peerStore.peerContacts(ih)
	if len(peerContacts) > 0 {
		l4g.Trace("replyGetPeers: Giving peers! %x was requested, and we knew %d peers!", ih, len(peerContacts))
	}
	return peerContacts
}

func (d *DHT) replyFindNode(addr *net.UDPAddr, r responseType) {
	totalRecvFindNode.Add(1)
	l4g.Trace(func() string {
		x := hashDistance(r.A.Target, d.nodeId)
		return fmt.Sprintf("DHT find_node. Host: %v , nodeId: %x , target ID: %x , distance to me: %x", addr, r.A.Id, r.A.Target, x)
	})

	node := r.A.Target
	r0 := map[string]interface{}{"id": d.nodeId}
	reply := replyMessage{
		T: r.T,
		Y: "r",
		R: r0,
	}

	// XXX we currently can't give out the peer contact. Probably requires
	// processing announce_peer.  XXX If there was a total match, that guy
	// is the last.
	neighbors := d.routingTable.lookup(node)
	n := make([]string, 0, kNodes)
	for _, r := range neighbors {
		n = append(n, r.id+r.addressBinaryFormat)
	}
	l4g.Trace("replyFindNode: Nodes only. Giving %d", len(n))
	reply.R["nodes"] = strings.Join(n, "")
	sendMsg(d.conn, addr, reply)
}

func (d *DHT) replyPing(addr *net.UDPAddr, response responseType) {
	l4g.Trace("DHT: reply ping => %v\n", addr)
	reply := replyMessage{
		T: response.T,
		Y: "r",
		R: map[string]interface{}{"id": d.nodeId},
	}
	sendMsg(d.conn, addr, reply)
}

// Process another node's response to a get_peers query. If the response
// contains peers, send them to the Torrent engine, our client, using the
// DHT.PeersRequestResults channel. If it contains closest nodes, query
// them if we still need it. Also announce ourselves as a peer for that node,
// unless we are in supernode mode.
func (d *DHT) processGetPeerResults(node *remoteNode, resp responseType) {
	totalRecvGetPeersReply.Add(1)
	query, _ := node.pendingQueries[resp.T]
	if d.peerStore.hasLocalDownload(query.ih) {
		d.announcePeer(node.address, query.ih, resp.R.Token)
	}
	if resp.R.Values != nil {
		peers := make([]string, 0)
		for _, peerContact := range resp.R.Values {
			if ok := d.peerStore.addContact(query.ih, peerContact); ok {
				peers = append(peers, peerContact)
			}
		}
		if len(peers) > 0 {
			// Finally, new peers.
			result := map[string][]string{query.ih: peers}
			totalPeers.Add(int64(len(peers)))
			l4g.Info("DHT: processGetPeerResults, totalPeers: %v", totalPeers.String())
			d.PeersRequestResults <- result
		}
	}
	if resp.R.Nodes != "" {
		for id, address := range parseNodesString(resp.R.Nodes) {

			if d.peerStore.count(query.ih) >= d.numTargetPeers {
				return
			}

			// If it's in our routing table already, ignore it.
			_, addr, existed, err := d.routingTable.hostPortToNode(address)
			if err != nil {
				l4g.Trace("DHT error parsing get peers node: %v", err)
				continue
			}
			if addr == node.address.String() {
				// This smartass is probably trying to
				// sniff the network, or attract a lot
				// of traffic to itself. Ignore all
				// their results.
				totalSelfPromotions.Add(1)
				continue
			}
			if existed {
				l4g.Trace(func() string {
					x := hashDistance(query.ih, node.id)
					return fmt.Sprintf("DHT: DUPE node reference: %x@%v from %x@%v. Distance: %x.", id, address, node.id, node.address.String(), x)
				})
				totalDupes.Add(1)
			} else {
				// And it is actually new. Interesting.
				l4g.Trace(func() string {
					x := hashDistance(query.ih, node.id)
					return fmt.Sprintf("DHT: Got new node reference: %x@%v from %x@%v. Distance: %x.", id, address, node.id, node.address.String(), x)
				})
				if _, err := d.routingTable.getOrCreateNode(id, addr); err == nil && d.peerStore.count(query.ih) < d.numTargetPeers {
					d.getPeers(query.ih)
				}
			}
		}
	}
}

// Process another node's response to a find_node query.
func (d *DHT) processFindNodeResults(node *remoteNode, resp responseType) {
	totalRecvFindNodeReply.Add(1)

	query, _ := node.pendingQueries[resp.T]

	if resp.R.Nodes != "" {
		for id, address := range parseNodesString(resp.R.Nodes) {
			_, addr, existed, err := d.routingTable.hostPortToNode(address)
			if err != nil {
				l4g.Trace("DHT error parsing node from find_find response: %v", err)
				continue
			}
			// SelfPromotions are more common for find_node. They are
			// happening even for router.bittorrent.com
			if existed {
				l4g.Trace(func() string {
					x := hashDistance(query.ih, node.id)
					return fmt.Sprintf("DHT: DUPE node reference: %x@%v from %x@%v. Distance: %x.", id, address, node.id, addr, x)
				})
				totalDupes.Add(1)
			} else {
				l4g.Trace(func() string {
					x := hashDistance(query.ih, node.id)
					return fmt.Sprintf("DHT: Got new node reference: %x@%v from %x@%v. Distance: %x.", id, address, node.id, addr, x)
				})
				if _, err := d.routingTable.getOrCreateNode(id, addr); err == nil {
					// Using d.findNode() instead of d.findNodeFrom() ensures
					// that only the closest neighbors are looked at.
					d.findNode(query.ih)
				}
			}
		}
	}
}

func randNodeId() []byte {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		l4g.Exit("nodeId rand:", err)
	}
	return b
}

var (
	expNodeIds                   = expvar.NewMap("nodeIds")
	totalReachableNodes          = expvar.NewInt("totalReachableNodes")
	totalDupes                   = expvar.NewInt("totalDupes")
	totalSelfPromotions          = expvar.NewInt("totalSelfPromotions")
	totalPeers                   = expvar.NewInt("totalPeers")
	totalSentPing                = expvar.NewInt("totalSentPing")
	totalSentGetPeers            = expvar.NewInt("totalSentGetPeers")
	totalSentFindNode            = expvar.NewInt("totalSentFindNode")
	totalRecvGetPeers            = expvar.NewInt("totalRecvGetPeers")
	totalRecvGetPeersReply       = expvar.NewInt("totalRecvGetPeersReply")
	totalRecvPingReply           = expvar.NewInt("totalRecvPingReply")
	totalRecvFindNode            = expvar.NewInt("totalRecvFindNode")
	totalRecvFindNodeReply       = expvar.NewInt("totalRecvFindNodeReply")
	totalPacketsFromBlockedHosts = expvar.NewInt("totalPacketsFromBlockedHosts")
	totalDroppedPackets          = expvar.NewInt("totalDroppedPackets")
	totalRecv                    = expvar.NewInt("totalRecv")
)

func init() {
	l4g.Global.AddFilter("stdout", l4g.WARNING, l4g.NewConsoleLogWriter())
}
