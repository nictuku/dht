// DHT node for Taipei Torrent, for tracker-less peer information exchange.
// Status: Supports all DHT operations from the specification.

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
	"crypto/sha1"
	"expvar"
	"flag"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	l4g "code.google.com/p/log4go"
	log "github.com/golang/glog"
	"github.com/nictuku/nettools"
)

var (
	dhtRouters    string
	maxNodes      int
	cleanupPeriod time.Duration
	savePeriod    time.Duration
	rateLimit     int64
)

func init() {
	flag.StringVar(&dhtRouters, "routers", "1.a.magnets.im:6881,router.utorrent.com:6881",
		"Comma separated IP:Port address of the DHT routeirs used to bootstrap the DHT network.")
	flag.IntVar(&maxNodes, "maxNodes", 500,
		"Maximum number of nodes to store in the routing table, in memory. This is the primary configuration for how noisy or aggressive this node should be. When the node starts, it will try to reach maxNodes/2 as quick as possible, to form a healthy routing table.")
	flag.DurationVar(&cleanupPeriod, "cleanupPeriod", 15*time.Minute,
		"How often to ping nodes in the network to see if they are reachable.")
	flag.DurationVar(&savePeriod, "savePeriod", 5*time.Minute,
		"How often to save the routing table to disk.")
	flag.Int64Var(&rateLimit, "rateLimit", 100,
		"Maximum packets per second to be processed. Beyond this limit they are silently dropped. Set to -1 to disable rate limiting.")

	// TODO: Control the verbosity via flag.
	// Setting during init has two purposes:
	// - Gives the calling program the ability to override this filter inside their main().
	// - Provides a sane default that isn't excessively verbose.
	l4g.AddFilter("stdout", l4g.WARNING, l4g.NewConsoleLogWriter())
}

const (
	// Try to ensure that at least these many nodes are in the routing table.
	minNodes           = 16
	secretRotatePeriod = 5 * time.Minute
)

// DHT should be created by NewDHTNode(). It provides DHT features to a
// torrent client, such as finding new peers for torrent downloads without
// requiring a tracker.
type DHT struct {
	nodeId string
	port   int

	routingTable *routingTable
	peerStore    *peerStore

	numTargetPeers int

	conn   *net.UDPConn
	Logger Logger

	exploredNeighborhood   bool
	remoteNodeAcquaintance chan string
	peersRequest           chan ihReq
	nodesRequest           chan ihReq
	pingRequest            chan *remoteNode
	stop                   chan bool
	clientThrottle         *nettools.ClientThrottle
	store                  *dhtStore
	tokenSecrets           []string

	// Public channels:
	PeersRequestResults chan map[InfoHash][]string // key = infohash, v = slice of peers.
}

func NewDHTNode(port, numTargetPeers int, storeEnabled bool) (node *DHT, err error) {
	node = &DHT{
		port:                 port,
		routingTable:         newRoutingTable(),
		peerStore:            newPeerStore(),
		PeersRequestResults:  make(chan map[InfoHash][]string, 1),
		stop:                 make(chan bool),
		exploredNeighborhood: false,
		// Buffer to avoid blocking on sends.
		remoteNodeAcquaintance: make(chan string, 100),
		// Buffer to avoid deadlocks and blocking on sends.
		peersRequest: make(chan ihReq, 100),
		nodesRequest: make(chan ihReq, 100),

		pingRequest: make(chan *remoteNode),

		numTargetPeers: numTargetPeers,
		clientThrottle: nettools.NewThrottler(),
		tokenSecrets:   []string{newTokenSecret(), newTokenSecret()},
	}
	c := openStore(port, storeEnabled)
	node.store = c
	if len(c.Id) != 20 {
		c.Id = randNodeId()
		log.Infof("newId: %x %d", c.Id, len(c.Id))
		saveStore(*c)
	}
	// The types don't match because JSON marshalling needs []byte.
	node.nodeId = string(c.Id)

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

func newTokenSecret() string {
	b := make([]byte, 5)
	if _, err := rand.Read(b); err != nil {
		// This would return a string with up to 5 null chars.
		log.Warningf("DHT: failed to generate random newTokenSecret: %v", err)
	}
	return string(b)
}

// Logger allows the DHT client to attach hooks for certain RPCs so it can log
// interesting events any way it wants.
type Logger interface {
	GetPeers(*net.UDPAddr, string, InfoHash)
}

type ihReq struct {
	ih       InfoHash
	announce bool
}

// PeersRequest asks the DHT to search for more peers for the infoHash
// provided. announce should be true if the connected peer is actively
// downloading this infohash, which is normally the case - unless this DHT node
// is just a router that doesn't downloads torrents.
func (d *DHT) PeersRequest(ih string, announce bool) {
	d.peersRequest <- ihReq{InfoHash(ih), announce}
	log.Infof("DHT: torrent client asking more peers for %x.", ih)
}

// Stop the DHT node.
func (d *DHT) Stop() {
	close(d.stop)
}

// Port returns the port number assigned to the DHT. This is useful when
// when initialising the DHT with port 0, i.e. automatic port assignment,
// in order to retrieve the actual port number used.
func (d *DHT) Port() int {
	return d.port
}

// AddNode informs the DHT of a new node it should add to its routing table.
// addr is a string containing the target node's "host:port" UDP address.
func (d *DHT) AddNode(addr string) {
	d.remoteNodeAcquaintance <- addr
}

// Asks for more peers for a torrent.
func (d *DHT) getPeers(infoHash InfoHash) {
	closest := d.routingTable.lookupFiltered(infoHash)
	for _, r := range closest {
		// *remoteNode is nil if it got filtered.
		if r != nil {
			d.getPeersFrom(r, infoHash)
		}
	}
}

// Find a DHT node.
func (d *DHT) findNode(id string) {
	ih := InfoHash(id)
	closest := d.routingTable.lookupFiltered(ih)
	for _, r := range closest {
		if r != nil {
			d.findNodeFrom(r, id)
		}
	}
}

// Run starts a DHT node. It bootstraps a routing table, if necessary, and
// listens for incoming DHT requests.
func (d *DHT) Run() error {
	socketChan := make(chan packetType)
	socket, err := listen(d.port)
	if err != nil {
		return err
	}
	d.conn = socket

	// Update the stored port number in case it was set 0, meaning it was
	// set automatically by the system
	d.port = socket.LocalAddr().(*net.UDPAddr).Port

	// There is goroutine pushing and one popping items out of the arena.
	// One passes work to the other. So there is little contention in the
	// arena, so it doesn't need many items (it used to have 500!). If
	// readFromSocket or the packet processing ever need to be
	// parallelized, this would have to be bumped.
	bytesArena := newArena(maxUDPPacketSize, 3)
	go readFromSocket(socket, socketChan, bytesArena, d.stop)

	// Bootstrap the network.
	for _, s := range strings.Split(dhtRouters, ",") {
		d.ping(s)
	}

	cleanupTicker := time.Tick(cleanupPeriod)
	secretRotateTicker := time.Tick(secretRotatePeriod)

	saveTicker := make(<-chan time.Time)
	if d.store != nil {
		saveTicker = time.Tick(savePeriod)
	}

	var fillTokenBucket <-chan time.Time
	tokenBucket := rateLimit

	if rateLimit < 0 {
		log.Warning("rate limiting disabled")
	} else {
		// Token bucket for limiting the number of packets per second.
		fillTokenBucket = time.Tick(time.Second / 10)
		if rateLimit > 0 && rateLimit < 10 {
			// Less than 10 leads to rounding problems.
			rateLimit = 10
		}
	}
	log.Infof("DHT: Starting DHT node %x on port %d.", d.nodeId, d.port)

	for {
		select {
		case <-d.stop:
			log.Infof("DHT exiting.")
			d.clientThrottle.Stop()
			log.Flush()
			return nil
		case addr := <-d.remoteNodeAcquaintance:
			d.helloFromPeer(addr)
		case req := <-d.peersRequest:
			// torrent server is asking for more peers for infoHash.  Ask the closest
			// nodes for directions. The goroutine will write into the
			// PeersNeededResults channel.

			// Drain all requests sitting in the channel and de-dupe them.
			m := map[InfoHash]bool{req.ih: req.announce}
		P:
			for {
				select {
				case req = <-d.peersRequest:
					m[req.ih] = req.announce
				default:
					// Channel drained.
					break P
				}
			}
			// Process each unique infohash for which there were requests.
			for ih, announce := range m {
				if announce {
					d.peerStore.addLocalDownload(ih)
				}

				if d.peerStore.count(ih) < d.numTargetPeers {
					d.getPeers(ih)
				}
			}

		case req := <-d.nodesRequest:
			m := map[InfoHash]bool{req.ih: true}
		L:
			for {
				select {
				case req = <-d.nodesRequest:
					m[req.ih] = true
				default:
					// Channel drained.
					break L
				}
			}
			for ih, _ := range m {
				d.findNode(string(ih))
			}

		case p := <-socketChan:
			totalRecv.Add(1)
			if rateLimit > 0 {
				if tokenBucket > 0 {
					d.processPacket(p)
					tokenBucket -= 1
				} else {
					// TODO In the future it might be better to avoid dropping things like ping replies.
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
			go pingSlowly(d.pingRequest, needPing, cleanupPeriod, d.stop)
		case node := <-d.pingRequest:
			d.pingNode(node)
		case <-secretRotateTicker:
			d.tokenSecrets = []string{newTokenSecret(), d.tokenSecrets[0]}
		case <-saveTicker:
			tbl := d.routingTable.reachableNodes()
			if len(tbl) > 5 {
				d.store.Remotes = tbl
				saveStore(*d.store)
			}
		}
	}
}

func (d *DHT) needMoreNodes() bool {
	n := d.routingTable.numNodes()
	return n < minNodes || n*2 < maxNodes
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
			log.Infof("DHT readResponse error processing response: %v", err)
			return
		}
		if !existed {
			log.Infof("DHT: Received reply from a host we don't know: %v", p.raddr)
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
				totalNodesReached.Add(1)
			}
			node.lastResponseTime = time.Now()
			node.pastQueries[r.T] = query
			d.routingTable.neighborhoodUpkeep(node)

			// If this is the first host added to the routing table, attempt a
			// recursive lookup of our own address, to build our neighborhood ASAP.
			if d.needMoreNodes() {
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
				log.Infof("DHT: Unknown query type: %v from %v", query.Type, addr)
			}
			delete(node.pendingQueries, r.T)
		} else {
			log.Infof("DHT: Unknown query id: %v", r.T)
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
		log.Infof("DHT: Bogus DHT query from %v.", p.raddr)
	}
}

func (d *DHT) ping(address string) {
	r, err := d.routingTable.getOrCreateNode("", address)
	if err != nil {
		log.Infof("ping error for address %v: %v", address, err)
		return
	}
	d.pingNode(r)
}

func (d *DHT) pingNode(r *remoteNode) {
	l4g.Debug("DHT: ping => %+v", r.address)
	t := r.newQuery("ping")

	queryArguments := map[string]interface{}{"id": d.nodeId}
	query := queryMessage{t, "q", "ping", queryArguments}
	sendMsg(d.conn, r.address, query)
	totalSentPing.Add(1)
}

func (d *DHT) getPeersFrom(r *remoteNode, ih InfoHash) {
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
		x := hashDistance(InfoHash(r.id), ih)
		return fmt.Sprintf("DHT sending get_peers. nodeID: %x@%v, InfoHash: %x , distance: %x", r.id, r.address, ih, x)
	})
	sendMsg(d.conn, r.address, query)
}

func (d *DHT) findNodeFrom(r *remoteNode, id string) {
	totalSentFindNode.Add(1)
	ty := "find_node"
	transId := r.newQuery(ty)
	ih := InfoHash(id)
	l4g.Trace("findNodeFrom adding pendingQueries transId=%v ih=%x", transId, ih)
	r.pendingQueries[transId].ih = ih
	queryArguments := map[string]interface{}{
		"id":     d.nodeId,
		"target": id,
	}
	query := queryMessage{transId, "q", ty, queryArguments}
	l4g.Trace(func() string {
		x := hashDistance(InfoHash(r.id), ih)
		return fmt.Sprintf("DHT sending find_node. nodeID: %x@%v, target ID: %x , distance: %x", r.id, r.address, id, x)
	})
	sendMsg(d.conn, r.address, query)
}

// announcePeer sends a message to the destination address to advertise that
// our node is a peer for this infohash, using the provided token to
// 'authenticate'.
func (d *DHT) announcePeer(address *net.UDPAddr, ih InfoHash, token string) {
	r, err := d.routingTable.getOrCreateNode("", address.String())
	if err != nil {
		l4g.Trace("announcePeer:", err)
		return
	}
	ty := "announce_peer"
	l4g.Trace("DHT: announce_peer => %v %x %x", address, ih, token)
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

func (d *DHT) hostToken(addr *net.UDPAddr, secret string) string {
	h := sha1.New()
	io.WriteString(h, addr.String())
	io.WriteString(h, secret)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (d *DHT) checkToken(addr *net.UDPAddr, token string) bool {
	match := false
	for _, secret := range d.tokenSecrets {
		if d.hostToken(addr, secret) == token {
			match = true
			break
		}
	}
	l4g.Trace("checkToken for %v, %q matches? %v", addr, token, match)
	return match
}

func (d *DHT) replyAnnouncePeer(addr *net.UDPAddr, r responseType) {
	ih := InfoHash(r.A.InfoHash)
	l4g.Trace(func() string {
		return fmt.Sprintf("DHT: announce_peer. Host %v, nodeID: %x, infoHash: %x, peerPort %d, distance to me %x",
			addr, r.A.Id, ih, r.A.Port, hashDistance(ih, InfoHash(d.nodeId)),
		)
	})
	if d.checkToken(addr, r.A.Token) {
		peerAddr := net.TCPAddr{IP: addr.IP, Port: r.A.Port}
		d.peerStore.addContact(ih, nettools.DottedPortToBinary(peerAddr.String()))
	}
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
	if log.V(2) {
		log.Infof("DHT get_peers. Host: %v , nodeID: %x , InfoHash: %x , distance to me: %x",
			addr, r.A.Id, InfoHash(r.A.InfoHash), hashDistance(r.A.InfoHash, InfoHash(d.nodeId)))
	}

	if d.Logger != nil {
		d.Logger.GetPeers(addr, r.A.Id, r.A.InfoHash)
	}

	ih := r.A.InfoHash
	r0 := map[string]interface{}{"id": d.nodeId, "token": d.hostToken(addr, d.tokenSecrets[0])}
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

func (d *DHT) nodesForInfoHash(ih InfoHash) string {
	n := make([]string, 0, kNodes)
	for _, r := range d.routingTable.lookup(ih) {
		// r is nil when the node was filtered.
		if r != nil {
			binaryHost := r.id + nettools.DottedPortToBinary(r.address.String())
			if binaryHost == "" {
				l4g.Trace("killing node with bogus address %v", r.address.String())
				d.routingTable.kill(r)
			} else {
				n = append(n, binaryHost)
			}
		}
	}
	l4g.Trace("replyGetPeers: Nodes only. Giving %d", len(n))
	return strings.Join(n, "")
}

func (d *DHT) peersForInfoHash(ih InfoHash) []string {
	peerContacts := d.peerStore.peerContacts(ih)
	if len(peerContacts) > 0 {
		l4g.Trace("replyGetPeers: Giving peers! %x was requested, and we knew %d peers!", ih, len(peerContacts))
	}
	return peerContacts
}

func (d *DHT) replyFindNode(addr *net.UDPAddr, r responseType) {
	totalRecvFindNode.Add(1)
	l4g.Trace(func() string {
		x := hashDistance(InfoHash(r.A.Target), InfoHash(d.nodeId))
		return fmt.Sprintf("DHT find_node. Host: %v , nodeId: %x , target ID: %x , distance to me: %x",
			addr, r.A.Id, r.A.Target, x)
	})

	node := InfoHash(r.A.Target)
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
	l4g.Trace("DHT: reply ping => %v", addr)
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
			result := map[InfoHash][]string{query.ih: peers}
			totalPeers.Add(int64(len(peers)))
			log.Infof("DHT: processGetPeerResults, totalPeers: %v", totalPeers.String())
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
					x := hashDistance(query.ih, InfoHash(node.id))
					return fmt.Sprintf("DHT: processGetPeerResults DUPE node reference: %x@%v from %x@%v. Distance: %x.",
						id, address, node.id, node.address, x)
				})
				totalGetPeersDupes.Add(1)
			} else {
				// And it is actually new. Interesting.
				l4g.Trace(func() string {
					x := hashDistance(query.ih, InfoHash(node.id))
					return fmt.Sprintf("DHT: Got new node reference: %x@%v from %x@%v. Distance: %x.",
						id, address, node.id, node.address, x)
				})
				_, err := d.routingTable.getOrCreateNode(id, addr)
				if err == nil && d.peerStore.count(query.ih) < d.numTargetPeers {
					// Re-add this request to the queue. This would in theory
					// batch similar requests, because new nodes are already
					// available in the routing table and will be used at the
					// next opportunity - before this particular channel send is
					// processed. As soon we reach target number of peers these
					// channel sends become noops.
					//
					// Setting the announce parameter to false because it's not
					// needed here: if this node is downloading that particular
					// infohash, that has already been recorded with
					// peerStore.addLocalDownload(). The announcement itself is
					// sent not when get_peers is sent, but when processing the
					// reply to get_peers.
					//
					select {
					case d.peersRequest <- ihReq{query.ih, false}:
					default:
						// The channel is full, so drop this item. The node
						// was added to the routing table already, so it
						// will be used next time getPeers() is called -
						// assuming it's close enough to the ih.
					}
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
			if addr == node.address.String() {
				// SelfPromotions are more common for find_node. They are
				// happening even for router.bittorrent.com
				totalSelfPromotions.Add(1)
				continue
			}
			if existed {
				l4g.Trace(func() string {
					x := hashDistance(query.ih, InfoHash(node.id))
					return fmt.Sprintf("DHT: processFindNodeResults DUPE node reference, query %x: %x@%v from %x@%v. Distance: %x.",
						query.ih, id, address, node.id, node.address, x)
				})
				totalFindNodeDupes.Add(1)
			} else {
				l4g.Trace(func() string {
					x := hashDistance(query.ih, InfoHash(node.id))
					return fmt.Sprintf("DHT: Got new node reference, query %x: %x@%v from %x@%v. Distance: %x.",
						query.ih, id, address, node.id, node.address, x)
				})
				// Includes the node in the routing table and ignores errors.
				//
				// Only continue the search if we really have to.
				_, err := d.routingTable.getOrCreateNode(id, addr)

				if err == nil && d.needMoreNodes() {
					select {
					case d.nodesRequest <- ihReq{query.ih, false}:
					default:
						// Too many find_node commands queued up. Dropping
						// this. The node has already been added to the
						// routing table so we're not losing any
						// information.
					}
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
	totalNodesReached            = expvar.NewInt("totalNodesReached")
	totalGetPeersDupes           = expvar.NewInt("totalGetPeersDupes")
	totalFindNodeDupes           = expvar.NewInt("totalFindNodeDupes")
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
