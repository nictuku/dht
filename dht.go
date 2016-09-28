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
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/nictuku/nettools"
)

// ===== Logging behavior =====
// By default nothing will be output on the screen, unless the program is
// called with special flags like -logtostderr. To show really important
// messages, use fmt.Print functions. The general rules for log verbosity are:
// - Major errors are logged with V(0) (or just ommit the V).
// - Rare events of major importance like the startup message should be logged
// with V(1).
// - Events related to user input (e.g: more peers requested, or peers found)
// should be logged with V(2).
// - Protocol errors should be logged with V(3). They are often noisy.
// - New incoming and outgoing KRPCs should also be logged with V(3).
// - Debugging and tracing should be logged with V(4) or higher.

// Config for the DHT Node. Use NewConfig to create a configuration with default values.
type Config struct {
	// IP Address to listen on.  If left blank, one is chosen automatically.
	Address string
	// UDP port the DHT node should listen on. If zero, it picks a random port.
	Port int
	// Number of peers that DHT will try to find for each infohash being searched. This might
	// later be moved to a per-infohash option. Default value: 5.
	NumTargetPeers int
	// Comma separated list of DHT routers used for bootstrapping the network.
	DHTRouters string
	// Maximum number of nodes to store in the routing table. Default value: 100.
	MaxNodes int
	// How often to ping nodes in the network to see if they are reachable. Default value: 15 min.
	CleanupPeriod time.Duration
	//  If true, the node will read the routing table from disk on startup and save routing
	//  table snapshots on disk every few minutes. Default value: true.
	SaveRoutingTable bool
	// How often to save the routing table to disk. Default value: 5 minutes.
	SavePeriod time.Duration
	// Maximum packets per second to be processed. Disabled if negative. Default value: 100.
	RateLimit int64
	// MaxInfoHashes is the limit of number of infohashes for which we should keep a peer list.
	// If this and MaxInfoHashPeers are unchanged, it should consume around 25 MB of RAM. Larger
	// values help keeping the DHT network healthy. Default value: 2048.
	MaxInfoHashes int
	// MaxInfoHashPeers is the limit of number of peers to be tracked for each infohash. A
	// single peer contact typically consumes 6 bytes. Default value: 256.
	MaxInfoHashPeers int
	// ClientPerMinuteLimit protects against spammy clients. Ignore their requests if exceeded
	// this number of packets per minute. Default value: 50.
	ClientPerMinuteLimit int
	// ThrottlerTrackedClients is the number of hosts the client throttler remembers. An LRU is used to
	// track the most interesting ones. Default value: 1000.
	ThrottlerTrackedClients int64
	//Protocol for UDP connections, udp4= IPv4, udp6 = IPv6
	UDPProto string
}

// Creates a *Config populated with default values.
func NewConfig() *Config {
	return &Config{
		Address:                 "",
		Port:                    0, // Picks a random port.
		NumTargetPeers:          5,
		DHTRouters:              "router.magnets.im:6881,router.bittorrent.com:6881,dht.transmissionbt.com:6881",
		MaxNodes:                500,
		CleanupPeriod:           15 * time.Minute,
		SaveRoutingTable:        true,
		SavePeriod:              5 * time.Minute,
		RateLimit:               100,
		MaxInfoHashes:           2048,
		MaxInfoHashPeers:        256,
		ClientPerMinuteLimit:    50,
		ThrottlerTrackedClients: 1000,
		UDPProto:                "udp4",
	}
}

var DefaultConfig = NewConfig()

// Registers Config fields as command line flags.  If c is nil, DefaultConfig
// is used.
func RegisterFlags(c *Config) {
	if c == nil {
		c = DefaultConfig
	}
	flag.StringVar(&c.DHTRouters, "routers", c.DHTRouters,
		"Comma separated addresses of DHT routers used to bootstrap the DHT network.")
	flag.IntVar(&c.MaxNodes, "maxNodes", c.MaxNodes,
		"Maximum number of nodes to store in the routing table, in memory. This is the primary configuration for how noisy or aggressive this node should be. When the node starts, it will try to reach d.config.MaxNodes/2 as quick as possible, to form a healthy routing table.")
	flag.DurationVar(&c.CleanupPeriod, "cleanupPeriod", c.CleanupPeriod,
		"How often to ping nodes in the network to see if they are reachable.")
	flag.DurationVar(&c.SavePeriod, "savePeriod", c.SavePeriod,
		"How often to save the routing table to disk.")
	flag.Int64Var(&c.RateLimit, "rateLimit", c.RateLimit,
		"Maximum packets per second to be processed. Beyond this limit they are silently dropped. Set to -1 to disable rate limiting.")
}

const (
	// Try to ensure that at least these many nodes are in the routing table.
	minNodes           = 16
	secretRotatePeriod = 5 * time.Minute
)

// DHT should be created by New(). It provides DHT features to a torrent
// client, such as finding new peers for torrent downloads without requiring a
// tracker.
type DHT struct {
	nodeId                 string
	config                 Config
	routingTable           *routingTable
	peerStore              *peerStore
	conn                   *net.UDPConn
	Logger                 Logger
	exploredNeighborhood   bool
	remoteNodeAcquaintance chan string
	peersRequest           chan ihReq
	nodesRequest           chan ihReq
	pingRequest            chan *remoteNode
	portRequest            chan int
	stop                   chan bool
	wg                     sync.WaitGroup
	clientThrottle         *nettools.ClientThrottle
	store                  *dhtStore
	tokenSecrets           []string

	// Public channels:
	PeersRequestResults chan map[InfoHash][]string // key = infohash, v = slice of peers.
}

// New creates a DHT node. If config is nil, DefaultConfig will be used.
// Changing the config after calling this function has no effect.
//
// This method replaces NewDHTNode.
func New(config *Config) (node *DHT, err error) {
	if config == nil {
		config = DefaultConfig
	}
	// Copy to avoid changes.
	cfg := *config
	node = &DHT{
		config:               cfg,
		routingTable:         newRoutingTable(),
		peerStore:            newPeerStore(cfg.MaxInfoHashes, cfg.MaxInfoHashPeers),
		PeersRequestResults:  make(chan map[InfoHash][]string, 1),
		stop:                 make(chan bool),
		exploredNeighborhood: false,
		// Buffer to avoid blocking on sends.
		remoteNodeAcquaintance: make(chan string, 100),
		// Buffer to avoid deadlocks and blocking on sends.
		peersRequest:   make(chan ihReq, 100),
		nodesRequest:   make(chan ihReq, 100),
		pingRequest:    make(chan *remoteNode),
		portRequest:    make(chan int),
		clientThrottle: nettools.NewThrottler(cfg.ClientPerMinuteLimit, cfg.ThrottlerTrackedClients),
		tokenSecrets:   []string{newTokenSecret(), newTokenSecret()},
	}
	c := openStore(cfg.Port, cfg.SaveRoutingTable)
	node.store = c
	if len(c.Id) != 20 {
		c.Id = randNodeId()
		log.V(4).Infof("Using a new random node ID: %x %d", c.Id, len(c.Id))
		saveStore(*c)
	}
	// The types don't match because JSON marshalling needs []byte.
	node.nodeId = string(c.Id)

	// XXX refactor.
	node.routingTable.nodeId = node.nodeId

	// This is called before the engine is up and ready to read from the
	// underlying channel.
	node.wg.Add(1)
	go func() {
		defer node.wg.Done()
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
	GetPeers(addr net.UDPAddr, queryID string, infoHash InfoHash)
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
	log.V(2).Infof("DHT: torrent client asking more peers for %x.", ih)
}

// Stop the DHT node.
func (d *DHT) Stop() {
	close(d.stop)
	d.wg.Wait()
}

// Port returns the port number assigned to the DHT. This is useful when
// when initialising the DHT with port 0, i.e. automatic port assignment,
// in order to retrieve the actual port number used.
func (d *DHT) Port() int {
	return <-d.portRequest
}

// AddNode informs the DHT of a new node it should add to its routing table.
// addr is a string containing the target node's "host:port" UDP address.
func (d *DHT) AddNode(addr string) {
	d.remoteNodeAcquaintance <- addr
}

// Asks for more peers for a torrent.
func (d *DHT) getPeers(infoHash InfoHash) {
	closest := d.routingTable.lookupFiltered(infoHash)
	if len(closest) == 0 {
		for _, s := range strings.Split(d.config.DHTRouters, ",") {
			if s != "" {
				r, e := d.routingTable.getOrCreateNode("", s, d.config.UDPProto)
				if e == nil {
					d.getPeersFrom(r, infoHash)
				}
			}
		}
	}
	for _, r := range closest {
		d.getPeersFrom(r, infoHash)
	}
}

// Find a DHT node.
func (d *DHT) findNode(id string) {
	ih := InfoHash(id)
	closest := d.routingTable.lookupFiltered(ih)
	if len(closest) == 0 {
		for _, s := range strings.Split(d.config.DHTRouters, ",") {
			if s != "" {
				r, e := d.routingTable.getOrCreateNode("", s, d.config.UDPProto)
				if e == nil {
					d.findNodeFrom(r, id)
				}
			}
		}
	}
	for _, r := range closest {
		d.findNodeFrom(r, id)
	}
}

// Start launches the dht node. It starts a listener
// on the desired address, then runs the main loop in a
// separate go routine - Start replaces Run and will
// always return, with nil if the dht successfully
// started or with an error either. d.Stop() is expected
// by the caller to stop the dht
func (d *DHT) Start() (err error) {
	if err = d.initSocket(); err == nil {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			d.loop()
		}()
	}
	return err
}

// Run launches the dht node. It starts a listener
// on the desired address, then runs the main loop in the
// same go routine.
// If initSocket fails, Run returns with the error.
// If initSocket succeeds, Run blocks until d.Stop() is called.
// DEPRECATED - Start should be used instead of Run
func (d *DHT) Run() error {
	log.Warning("dht.Run() is deprecated, use dht.Start() instead")
	if err := d.initSocket(); err != nil {
		return err
	}
	d.loop()
	return nil
}

// initSocket initializes the udp socket
// listening to incoming dht requests
func (d *DHT) initSocket() (err error) {
	d.conn, err = listen(d.config.Address, d.config.Port, d.config.UDPProto)
	if err != nil {
		return err
	}

	// Update the stored port number in case it was set 0, meaning it was
	// set automatically by the system
	d.config.Port = d.conn.LocalAddr().(*net.UDPAddr).Port
	return nil
}

func (d *DHT) bootstrap() {
	// Bootstrap the network (only if there are configured dht routers).
	for _, s := range strings.Split(d.config.DHTRouters, ",") {
		if s != "" {
			d.ping(s)
			r, e := d.routingTable.getOrCreateNode("", s, d.config.UDPProto)
			if e == nil {
				d.findNodeFrom(r, d.nodeId)
			}
		}
	}
	d.findNode(d.nodeId)
	d.getMorePeers(nil)
}

// loop is the main working section of dht.
// It bootstraps a routing table, if necessary,
// and listens for incoming DHT requests until d.Stop()
// is called from another go routine.
func (d *DHT) loop() {
	// Close socket
	defer d.conn.Close()

	// There is goroutine pushing and one popping items out of the arena.
	// One passes work to the other. So there is little contention in the
	// arena, so it doesn't need many items (it used to have 500!). If
	// readFromSocket or the packet processing ever need to be
	// parallelized, this would have to be bumped.
	bytesArena := newArena(maxUDPPacketSize, 3)
	socketChan := make(chan packetType)
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		readFromSocket(d.conn, socketChan, bytesArena, d.stop)
	}()

	d.bootstrap()

	cleanupTicker := time.Tick(d.config.CleanupPeriod)
	secretRotateTicker := time.Tick(secretRotatePeriod)

	saveTicker := make(<-chan time.Time)
	if d.store != nil {
		saveTicker = time.Tick(d.config.SavePeriod)
	}

	var fillTokenBucket <-chan time.Time
	tokenBucket := d.config.RateLimit

	if d.config.RateLimit < 0 {
		log.Warning("rate limiting disabled")
	} else {
		// Token bucket for limiting the number of packets per second.
		fillTokenBucket = time.Tick(time.Second / 10)
		if d.config.RateLimit > 0 && d.config.RateLimit < 10 {
			// Less than 10 leads to rounding problems.
			d.config.RateLimit = 10
		}
	}
	log.V(1).Infof("DHT: Starting DHT node %x on port %d.", d.nodeId, d.config.Port)

	for {
		select {
		case <-d.stop:
			log.V(1).Infof("DHT exiting.")
			d.clientThrottle.Stop()
			log.Flush()
			return
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

				d.getPeers(ih) // I might have enough peers in the peerstore, but no seeds
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
			if d.config.RateLimit > 0 {
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
			if tokenBucket < d.config.RateLimit {
				tokenBucket += d.config.RateLimit / 10
			}
		case <-cleanupTicker:
			needPing := d.routingTable.cleanup(d.config.CleanupPeriod, d.peerStore)
			d.wg.Add(1)
			go func() {
				defer d.wg.Done()
				pingSlowly(d.pingRequest, needPing, d.config.CleanupPeriod, d.stop)
			}()
			if d.needMoreNodes() {
				d.bootstrap()
			}
		case node := <-d.pingRequest:
			d.pingNode(node)
		case <-secretRotateTicker:
			d.tokenSecrets = []string{newTokenSecret(), d.tokenSecrets[0]}
		case d.portRequest <- d.config.Port:
			continue
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
	return n < minNodes || n*2 < d.config.MaxNodes
}

func (d *DHT) needMorePeers(ih InfoHash) bool {
	return d.peerStore.alive(ih) < d.config.NumTargetPeers
}

func (d *DHT) getMorePeers(r *remoteNode) {
	for ih := range d.peerStore.localActiveDownloads {
		if d.needMorePeers(ih) {
			if r == nil {
				d.getPeers(ih)
			} else {
				d.getPeersFrom(r, ih)
			}
		}
	}
}

func (d *DHT) helloFromPeer(addr string) {
	// We've got a new node id. We need to:
	// - see if we know it already, skip accordingly.
	// - ping it and see if it's reachable.
	// - if it responds, save it in the routing table.
	_, addrResolved, existed, err := d.routingTable.hostPortToNode(addr, d.config.UDPProto)
	if err != nil {
		log.Warningf("helloFromPeer error: %v", err)
		return
	}
	if existed {
		// Node host+port already known.
		return
	}
	if d.routingTable.length() < d.config.MaxNodes {
		d.ping(addrResolved)
		return
	}
}

func (d *DHT) processPacket(p packetType) {
	log.V(5).Infof("DHT processing packet from %v", p.raddr.String())
	if !d.clientThrottle.CheckBlock(p.raddr.IP.String()) {
		totalPacketsFromBlockedHosts.Add(1)
		log.V(5).Infof("Node exceeded rate limiter. Dropping packet.")
		return
	}
	if p.b[0] != 'd' {
		// Malformed DHT packet. There are protocol extensions out
		// there that we don't support or understand.
		log.V(5).Infof("Malformed DHT packet.")
		return
	}
	r, err := readResponse(p)
	if err != nil {
		log.Warningf("DHT: readResponse Error: %v, %q", err, string(p.b))
		return
	}
	switch {
	// Response.
	case r.Y == "r":
		log.V(5).Infof("DHT processing response from %x", r.R.Id)
		if bogusId(r.R.Id) {
			log.V(3).Infof("DHT received packet with bogus node id %x", r.R.Id)
			return
		}
		if r.R.Id == d.nodeId {
			log.V(3).Infof("DHT received reply from self, id %x", r.A.Id)
			return
		}
		node, addr, existed, err := d.routingTable.hostPortToNode(p.raddr.String(), d.config.UDPProto)
		if err != nil {
			log.V(3).Infof("DHT readResponse error processing response: %v", err)
			return
		}
		if !existed {
			log.V(3).Infof("DHT: Received reply from a host we don't know: %v", p.raddr)
			if d.routingTable.length() < d.config.MaxNodes {
				d.ping(addr)
			}
			return
		}
		// Fix the node ID.
		if node.id == "" {
			node.id = r.R.Id
			d.routingTable.update(node, d.config.UDPProto)
		}
		if node.id != r.R.Id {
			log.V(3).Infof("DHT: Node changed IDs %x => %x", node.id, r.R.Id)
		}
		if query, ok := node.pendingQueries[r.T]; ok {
			log.V(4).Infof("DHT: Received reply to %v", query.Type)
			if !node.reachable {
				node.reachable = true
				totalNodesReached.Add(1)
			}
			node.lastResponseTime = time.Now()
			node.pastQueries[r.T] = query
			d.routingTable.neighborhoodUpkeep(node, d.config.UDPProto, d.peerStore)

			// If this is the first host added to the routing table, attempt a
			// recursive lookup of our own address, to build our neighborhood ASAP.
			if d.needMoreNodes() {
				log.V(5).Infof("DHT: need more nodes")
				d.findNode(d.nodeId)
			}
			d.exploredNeighborhood = true

			switch query.Type {
			case "ping":
				// Served its purpose, nothing else to be done.
				totalRecvPingReply.Add(1)
			case "get_peers":
				log.V(5).Infof("DHT: got get_peers response")
				d.processGetPeerResults(node, r)
			case "find_node":
				log.V(5).Infof("DHT: got find_node response")
				d.processFindNodeResults(node, r)
			case "announce_peer":
				// Nothing to do. In the future, update counters.
			default:
				log.V(3).Infof("DHT: Unknown query type: %v from %v", query.Type, addr)
			}
			delete(node.pendingQueries, r.T)
		} else {
			log.V(3).Infof("DHT: Unknown query id: %v", r.T)
		}
	case r.Y == "q":
		if r.A.Id == d.nodeId {
			log.V(3).Infof("DHT received packet from self, id %x", r.A.Id)
			return
		}
		node, addr, existed, err := d.routingTable.hostPortToNode(p.raddr.String(), d.config.UDPProto)
		if err != nil {
			log.Warningf("Error readResponse error processing query: %v", err)
			return
		}
		if !existed {
			// Another candidate for the routing table. See if it's reachable.
			if d.routingTable.length() < d.config.MaxNodes {
				d.ping(addr)
			}
		}
		log.V(5).Infof("DHT processing %v request", r.Q)
		switch r.Q {
		case "ping":
			d.replyPing(p.raddr, r)
		case "get_peers":
			d.replyGetPeers(p.raddr, r)
		case "find_node":
			d.replyFindNode(p.raddr, r)
		case "announce_peer":
			d.replyAnnouncePeer(p.raddr, node, r)
		default:
			log.V(3).Infof("DHT: non-implemented handler for type %v", r.Q)
		}
	default:
		log.V(3).Infof("DHT: Bogus DHT query from %v.", p.raddr)
	}
}

func (d *DHT) ping(address string) {
	r, err := d.routingTable.getOrCreateNode("", address, d.config.UDPProto)
	if err != nil {
		log.V(3).Infof("ping error for address %v: %v", address, err)
		return
	}
	d.pingNode(r)
}

func (d *DHT) pingNode(r *remoteNode) {
	log.V(3).Infof("DHT: ping => %+v", r.address)
	t := r.newQuery("ping")

	queryArguments := map[string]interface{}{"id": d.nodeId}
	query := queryMessage{t, "q", "ping", queryArguments}
	sendMsg(d.conn, r.address, query)
	totalSentPing.Add(1)
}

func (d *DHT) getPeersFrom(r *remoteNode, ih InfoHash) {
	if r == nil {
		return
	}
	totalSentGetPeers.Add(1)
	ty := "get_peers"
	transId := r.newQuery(ty)
	if _, ok := r.pendingQueries[transId]; ok {
		r.pendingQueries[transId].ih = ih
	} else {
		r.pendingQueries[transId] = &queryType{ih: ih}
	}
	queryArguments := map[string]interface{}{
		"id":        d.nodeId,
		"info_hash": ih,
	}
	query := queryMessage{transId, "q", ty, queryArguments}
	if log.V(3) {
		x := hashDistance(InfoHash(r.id), ih)
		log.V(3).Infof("DHT sending get_peers. nodeID: %x@%v, InfoHash: %x , distance: %x", r.id, r.address, ih, x)
	}
	r.lastSearchTime = time.Now()
	sendMsg(d.conn, r.address, query)
}

func (d *DHT) findNodeFrom(r *remoteNode, id string) {
	if r == nil {
		return
	}
	totalSentFindNode.Add(1)
	ty := "find_node"
	transId := r.newQuery(ty)
	ih := InfoHash(id)
	log.V(3).Infof("findNodeFrom adding pendingQueries transId=%v ih=%x", transId, ih)
	if _, ok := r.pendingQueries[transId]; ok {
		r.pendingQueries[transId].ih = ih
	} else {
		r.pendingQueries[transId] = &queryType{ih: ih}
	}
	queryArguments := map[string]interface{}{
		"id":     d.nodeId,
		"target": id,
	}
	query := queryMessage{transId, "q", ty, queryArguments}
	if log.V(3) {
		x := hashDistance(InfoHash(r.id), ih)
		log.V(3).Infof("DHT sending find_node. nodeID: %x@%v, target ID: %x , distance: %x", r.id, r.address, id, x)
	}
	r.lastSearchTime = time.Now()
	sendMsg(d.conn, r.address, query)
}

// announcePeer sends a message to the destination address to advertise that
// our node is a peer for this infohash, using the provided token to
// 'authenticate'.
func (d *DHT) announcePeer(address net.UDPAddr, ih InfoHash, token string) {
	r, err := d.routingTable.getOrCreateNode("", address.String(), d.config.UDPProto)
	if err != nil {
		log.V(3).Infof("announcePeer error: %v", err)
		return
	}
	ty := "announce_peer"
	log.V(3).Infof("DHT: announce_peer => address: %v, ih: %x, token: %x", address, ih, token)
	transId := r.newQuery(ty)
	queryArguments := map[string]interface{}{
		"id":        d.nodeId,
		"info_hash": ih,
		"port":      d.config.Port,
		"token":     token,
	}
	query := queryMessage{transId, "q", ty, queryArguments}
	sendMsg(d.conn, address, query)
}

func (d *DHT) hostToken(addr net.UDPAddr, secret string) string {
	h := sha1.New()
	io.WriteString(h, addr.String())
	io.WriteString(h, secret)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (d *DHT) checkToken(addr net.UDPAddr, token string) bool {
	match := false
	for _, secret := range d.tokenSecrets {
		if d.hostToken(addr, secret) == token {
			match = true
			break
		}
	}
	log.V(4).Infof("checkToken for %v, %q matches? %v", addr, token, match)
	return match
}

func (d *DHT) replyAnnouncePeer(addr net.UDPAddr, node *remoteNode, r responseType) {
	ih := InfoHash(r.A.InfoHash)
	if log.V(3) {
		log.Infof("DHT: announce_peer. Host %v, nodeID: %x, infoHash: %x, peerPort %d, distance to me %x",
			addr, r.A.Id, ih, r.A.Port, hashDistance(ih, InfoHash(d.nodeId)),
		)
	}
	// node can be nil if, for example, the server just restarted and received an announce_peer
	// from a node it doesn't yet know about.
	if node != nil && d.checkToken(addr, r.A.Token) {
		peerAddr := net.TCPAddr{IP: addr.IP, Port: r.A.Port}
		d.peerStore.addContact(ih, nettools.DottedPortToBinary(peerAddr.String()))
		// Allow searching this node immediately, since it's telling us
		// it has an infohash. Enables faster upgrade of other nodes to
		// "peer" of an infohash, if the announcement is valid.
		node.lastResponseTime = time.Now().Add(-searchRetryPeriod)
		if d.peerStore.hasLocalDownload(ih) {
			d.PeersRequestResults <- map[InfoHash][]string{ih: []string{nettools.DottedPortToBinary(peerAddr.String())}}
		}
	}
	// Always reply positively. jech says this is to avoid "back-tracking", not sure what that means.
	reply := replyMessage{
		T: r.T,
		Y: "r",
		R: map[string]interface{}{"id": d.nodeId},
	}
	sendMsg(d.conn, addr, reply)
}

func (d *DHT) replyGetPeers(addr net.UDPAddr, r responseType) {
	totalRecvGetPeers.Add(1)
	if log.V(3) {
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
				log.V(3).Infof("killing node with bogus address %v", r.address.String())
				d.routingTable.kill(r, d.peerStore)
			} else {
				n = append(n, binaryHost)
			}
		}
	}
	log.V(3).Infof("replyGetPeers: Nodes only. Giving %d", len(n))
	return strings.Join(n, "")
}

func (d *DHT) peersForInfoHash(ih InfoHash) []string {
	peerContacts := d.peerStore.peerContacts(ih)
	if len(peerContacts) > 0 {
		log.V(3).Infof("replyGetPeers: Giving peers! %x was requested, and we knew %d peers!", ih, len(peerContacts))
	}
	return peerContacts
}

func (d *DHT) replyFindNode(addr net.UDPAddr, r responseType) {
	totalRecvFindNode.Add(1)
	if log.V(3) {
		x := hashDistance(InfoHash(r.A.Target), InfoHash(d.nodeId))
		log.Infof("DHT find_node. Host: %v , nodeId: %x , target ID: %x , distance to me: %x",
			addr, r.A.Id, r.A.Target, x)
	}

	node := InfoHash(r.A.Target)
	r0 := map[string]interface{}{"id": d.nodeId}
	reply := replyMessage{
		T: r.T,
		Y: "r",
		R: r0,
	}

	neighbors := d.routingTable.lookupFiltered(node)
	if len(neighbors) < kNodes {
		neighbors = append(neighbors, d.routingTable.lookup(node)...)
	}
	n := make([]string, 0, kNodes)
	for _, r := range neighbors {
		n = append(n, r.id+r.addressBinaryFormat)
		if len(n) == kNodes {
			break
		}
	}
	log.V(3).Infof("replyFindNode: Nodes only. Giving %d", len(n))
	reply.R["nodes"] = strings.Join(n, "")
	sendMsg(d.conn, addr, reply)
}

func (d *DHT) replyPing(addr net.UDPAddr, response responseType) {
	log.V(3).Infof("DHT: reply ping => %v", addr)
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
			// send peer even if we already have it in store
			// the underlying client does/should handle dupes
			d.peerStore.addContact(query.ih, peerContact)
			peers = append(peers, peerContact)
		}
		if len(peers) > 0 {
			// Finally, new peers.
			result := map[InfoHash][]string{query.ih: peers}
			totalPeers.Add(int64(len(peers)))
			log.V(2).Infof("DHT: processGetPeerResults, totalPeers: %v", totalPeers.String())
			select {
			case d.PeersRequestResults <- result:
			case <-d.stop:
				// if we're closing down and the caller has stopped reading
				// from PeersRequestResults, drop the result.
			}
		}
	}
	var nodelist string

	if d.config.UDPProto == "udp4" {
		nodelist = resp.R.Nodes
	} else if d.config.UDPProto == "udp6" {
		nodelist = resp.R.Nodes6
	}
	log.V(5).Infof("DHT: handling get_peers results len(nodelist)=%d", len(nodelist))
	if nodelist != "" {
		for id, address := range parseNodesString(nodelist, d.config.UDPProto) {
			if id == d.nodeId {
				log.V(5).Infof("DHT got reference of self for get_peers, id %x", id)
				continue
			}

			// If it's in our routing table already, ignore it.
			_, addr, existed, err := d.routingTable.hostPortToNode(address, d.config.UDPProto)
			if err != nil {
				log.V(3).Infof("DHT error parsing get peers node: %v", err)
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
				if log.V(4) {
					x := hashDistance(query.ih, InfoHash(node.id))
					log.Infof("DHT: processGetPeerResults DUPE node reference: %x@%v from %x@%v. Distance: %x.",
						id, address, node.id, node.address, x)
				}
				totalGetPeersDupes.Add(1)
			} else {
				// And it is actually new. Interesting.
				if log.V(4) {
					x := hashDistance(query.ih, InfoHash(node.id))
					log.Infof("DHT: Got new node reference: %x@%v from %x@%v. Distance: %x.",
						id, address, node.id, node.address, x)
				}
				if _, err := d.routingTable.getOrCreateNode(id, addr, d.config.UDPProto); err == nil && d.needMorePeers(query.ih) {
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
	var nodelist string
	totalRecvFindNodeReply.Add(1)

	query, _ := node.pendingQueries[resp.T]
	if d.config.UDPProto == "udp4" {
		nodelist = resp.R.Nodes
	} else if d.config.UDPProto == "udp6" {
		nodelist = resp.R.Nodes6
	}
	log.V(5).Infof("processFindNodeResults find_node = %s len(nodelist)=%d", nettools.BinaryToDottedPort(node.addressBinaryFormat), len(nodelist))

	if nodelist != "" {
		for id, address := range parseNodesString(nodelist, d.config.UDPProto) {
			_, addr, existed, err := d.routingTable.hostPortToNode(address, d.config.UDPProto)
			if err != nil {
				log.V(3).Infof("DHT error parsing node from find_find response: %v", err)
				continue
			}
			if id == d.nodeId {
				log.V(5).Infof("DHT got reference of self for find_node, id %x", id)
				continue
			}
			if addr == node.address.String() {
				// SelfPromotions are more common for find_node. They are
				// happening even for router.bittorrent.com
				totalSelfPromotions.Add(1)
				continue
			}
			if existed {
				if log.V(4) {
					x := hashDistance(query.ih, InfoHash(node.id))
					log.Infof("DHT: processFindNodeResults DUPE node reference, query %x: %x@%v from %x@%v. Distance: %x.",
						query.ih, id, address, node.id, node.address, x)
				}
				totalFindNodeDupes.Add(1)
			} else {
				if log.V(4) {
					x := hashDistance(query.ih, InfoHash(node.id))
					log.Infof("DHT: Got new node reference, query %x: %x@%v from %x@%v. Distance: %x.",
						query.ih, id, address, node.id, node.address, x)
				}
				// Includes the node in the routing table and ignores errors.
				//
				// Only continue the search if we really have to.
				r, err := d.routingTable.getOrCreateNode(id, addr, d.config.UDPProto)
				if err != nil {
					log.Warningf("processFindNodeResults calling getOrCreateNode: %v. Id=%x, Address=%q", err, id, addr)
					continue
				}
				if d.needMoreNodes() {
					select {
					case d.nodesRequest <- ihReq{query.ih, false}:
					default:
						// Too many find_node commands queued up. Dropping
						// this. The node has already been added to the
						// routing table so we're not losing any
						// information.
					}
				}
				d.getMorePeers(r)
			}
		}
	}
}

func randNodeId() []byte {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		log.Fatalln("nodeId rand:", err)
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
