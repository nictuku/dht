package dht

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"expvar"
	"fmt"
	"net"
	"strconv"
	"time"

	log "github.com/golang/glog"
	bencode "github.com/jackpal/bencode-go"
	"github.com/nictuku/nettools"
)

// Search a node again after some time.
var searchRetryPeriod = 15 * time.Second

// Owned by the DHT engine.
type remoteNode struct {
	address net.UDPAddr
	// addressDotFormatted contains a binary representation of the node's host:port address.
	addressBinaryFormat string
	id                  string
	// lastQueryID should be incremented after consumed. Based on the
	// protocol, it would be two letters, but I'm using 0-255, although
	// treated as string.
	lastQueryID int
	// TODO: key by infohash instead?
	pendingQueries   map[string]*queryType // key: transaction ID
	pastQueries      map[string]*queryType // key: transaction ID
	reachable        bool
	lastResponseTime time.Time
	lastSearchTime   time.Time
	ActiveDownloads  []string // List of infohashes we know this peer is downloading.
}

func newRemoteNode(addr net.UDPAddr, id string) *remoteNode {
	return &remoteNode{
		address:             addr,
		addressBinaryFormat: nettools.DottedPortToBinary(addr.String()),
		lastQueryID:         newTransactionId(),
		id:                  id,
		reachable:           false,
		pendingQueries:      map[string]*queryType{},
		pastQueries:         map[string]*queryType{},
	}
}

type queryType struct {
	Type    string
	ih      InfoHash
	srcNode string
}

const (
	// Once in a while I get a few bigger ones, but meh.
	maxUDPPacketSize = 4096
	v4nodeContactLen = 26
	v6nodeContactLen = 38 // some clients seem to send multiples of 38
	nodeIdLen        = 20
)

var (
	totalSent         = expvar.NewInt("totalSent")
	totalReadBytes    = expvar.NewInt("totalReadBytes")
	totalWrittenBytes = expvar.NewInt("totalWrittenBytes")
)

// The 'nodes' response is a string with fixed length contacts concatenated arbitrarily.
func parseNodesString(nodes string, proto string) (parsed map[string]string) {
	var nodeContactLen int
	if proto == "udp4" {
		nodeContactLen = v4nodeContactLen
	} else if proto == "udp6" {
		nodeContactLen = v6nodeContactLen
	} else {
		return
	}
	parsed = make(map[string]string)
	if len(nodes)%nodeContactLen > 0 {
		log.V(3).Infof("DHT: len(NodeString) = %d, INVALID LENGTH, should be a multiple of %d", len(nodes), nodeContactLen)
		log.V(5).Infof("%T %#v\n", nodes, nodes)
		return
	} else {
		log.V(5).Infof("DHT: len(NodeString) = %d, had %d nodes, nodeContactLen=%d\n", len(nodes), len(nodes)/nodeContactLen, nodeContactLen)
	}
	for i := 0; i < len(nodes); i += nodeContactLen {
		id := nodes[i : i+nodeIdLen]
		address := nettools.BinaryToDottedPort(nodes[i+nodeIdLen : i+nodeContactLen])
		parsed[id] = address
	}
	return

}

// newQuery creates a new transaction id and adds an entry to r.pendingQueries.
// It does not set any extra information to the transaction information, so the
// caller must take care of that.
func (r *remoteNode) newQuery(transType string) (transId string) {
	log.V(4).Infof("newQuery for %x, lastID %v", r.id, r.lastQueryID)
	r.lastQueryID = (r.lastQueryID + 1) % 256
	transId = strconv.Itoa(r.lastQueryID)
	log.V(4).Infof("... new id %v", r.lastQueryID)
	r.pendingQueries[transId] = &queryType{Type: transType}
	return
}

// wasContactedRecently returns true if a node was contacted recently _and_
// one of the recent queries (not necessarily the last) was about the ih. If
// the ih is different at each time, it will keep returning false.
func (r *remoteNode) wasContactedRecently(ih InfoHash) bool {
	if len(r.pendingQueries) == 0 && len(r.pastQueries) == 0 {
		return false
	}
	if !r.lastResponseTime.IsZero() && time.Since(r.lastResponseTime) > searchRetryPeriod {
		return false
	}
	for _, q := range r.pendingQueries {
		if q.ih == ih {
			return true
		}
	}
	if !r.lastSearchTime.IsZero() && time.Since(r.lastSearchTime) > searchRetryPeriod {
		return false
	}
	for _, q := range r.pastQueries {
		if q.ih == ih {
			return true
		}
	}
	return false
}

type getPeersResponse struct {
	// TODO: argh, values can be a string depending on the client (e.g: original bittorrent).
	Values []string "values"
	Id     string   "id"
	Nodes  string   "nodes"
	Nodes6 string   "nodes6"
	Token  string   "token"
}

type answerType struct {
	Id       string   "id"
	Target   string   "target"
	InfoHash InfoHash "info_hash" // should probably be a string.
	Port     int      "port"
	Token    string   "token"
}

// Generic stuff we read from the wire, not knowing what it is. This is as generic as can be.
type responseType struct {
	T string           "t"
	Y string           "y"
	Q string           "q"
	R getPeersResponse "r"
	E []string         "e"
	A answerType       "a"
	// Unsupported mainline extension for client identification.
	// V string(?)	"v"
}

// sendMsg bencodes the data in 'query' and sends it to the remote node.
func sendMsg(conn *net.UDPConn, raddr net.UDPAddr, query interface{}) {
	totalSent.Add(1)
	var b bytes.Buffer
	if err := bencode.Marshal(&b, query); err != nil {
		return
	}
	if n, err := conn.WriteToUDP(b.Bytes(), &raddr); err != nil {
		log.V(3).Infof("DHT: node write failed to %+v, error=%s", raddr, err)
	} else {
		totalWrittenBytes.Add(int64(n))
	}
	return
}

// Read responses from bencode-speaking nodes. Return the appropriate data structure.
func readResponse(p packetType) (response responseType, err error) {
	// The calls to bencode.Unmarshal() can be fragile.
	defer func() {
		if x := recover(); x != nil {
			log.V(3).Infof("DHT: !!! Recovering from panic() after bencode.Unmarshal %q, %v", string(p.b), x)
		}
	}()
	if e2 := bencode.Unmarshal(bytes.NewBuffer(p.b), &response); e2 == nil {
		err = nil
		return
	} else {
		log.V(3).Infof("DHT: unmarshal error, odd or partial data during UDP read? %v, err=%s", string(p.b), e2)
		return response, e2
	}
	return
}

// Message to be sent out in the wire. Must not have any extra fields.
type queryMessage struct {
	T string                 "t"
	Y string                 "y"
	Q string                 "q"
	A map[string]interface{} "a"
}

type replyMessage struct {
	T string                 "t"
	Y string                 "y"
	R map[string]interface{} "r"
}

type packetType struct {
	b     []byte
	raddr net.UDPAddr
}

func listen(addr string, listenPort int, proto string) (socket *net.UDPConn, err error) {
	log.V(3).Infof("DHT: Listening for peers on IP: %s port: %d Protocol=%s\n", addr, listenPort, proto)
	listener, err := net.ListenPacket(proto, addr+":"+strconv.Itoa(listenPort))
	if err != nil {
		log.V(3).Infof("DHT: Listen failed:", err)
	}
	if listener != nil {
		socket = listener.(*net.UDPConn)
	}
	return
}

// Read from UDP socket, writes slice of byte into channel.
func readFromSocket(socket *net.UDPConn, conChan chan packetType, bytesArena arena, stop chan bool) {
	for {
		b := bytesArena.Pop()
		n, addr, err := socket.ReadFromUDP(b)
		if err != nil {
			log.V(3).Infof("DHT: readResponse error:", err)
		}
		b = b[0:n]
		if n == maxUDPPacketSize {
			log.V(3).Infof("DHT: Warning. Received packet with len >= %d, some data may have been discarded.\n", maxUDPPacketSize)
		}
		totalReadBytes.Add(int64(n))
		if n > 0 && err == nil {
			p := packetType{b, *addr}
			select {
			case conChan <- p:
				continue
			case <-stop:
				return
			}
		}
		// Do a non-blocking read of the stop channel and stop this goroutine if the channel
		// has been closed.
		select {
		case <-stop:
			return
		default:
		}
	}
}

func bogusId(id string) bool {
	return len(id) != 20
}

func newTransactionId() int {
	n, err := rand.Read(make([]byte, 1))
	if err != nil {
		return time.Now().Second()
	}
	return n
}

type InfoHash string

func (i InfoHash) String() string {
	return fmt.Sprintf("%x", string(i))
}

// DecodeInfoHash transforms a hex-encoded 20-characters string to a binary
// infohash.
func DecodeInfoHash(x string) (b InfoHash, err error) {
	var h []byte
	h, err = hex.DecodeString(x)
	if len(h) != 20 {
		return "", fmt.Errorf("DecodeInfoHash: expected InfoHash len=20, got %d", len(h))
	}
	return InfoHash(h), err
}

// DecodePeerAddress transforms the binary-encoded host:port address into a
// human-readable format. So, "abcdef" becomes 97.98.99.100:25958.
func DecodePeerAddress(x string) string {
	return nettools.BinaryToDottedPort(x)
}
