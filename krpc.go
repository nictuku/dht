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

	bencode "code.google.com/p/bencode-go"
	l4g "code.google.com/p/log4go"
	"github.com/nictuku/nettools"
)

// Owned by the DHT engine.
type remoteNode struct {
	address *net.UDPAddr
	// addressDotFormatted contains a binary representation of the node's host:port address.
	addressBinaryFormat string
	id                  string
	// lastQueryID should be incremented after consumed. Based on the
	// protocol, it would be two letters, but I'm using 0-255, although
	// treated as string.
	lastQueryID     int
	pendingQueries  map[string]*queryType // key: transaction ID
	pastQueries     map[string]*queryType // key: transaction ID
	reachable       bool
	lastTime        time.Time
	ActiveDownloads []string // List of infohashes we know this peer is downloading.
}

func newRemoteNode(addr *net.UDPAddr, id string) *remoteNode {
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
	nodeContactLen   = 26
	nodeIdLen        = 20
)

var (
	totalSent = expvar.NewInt("totalSent")
)

// The 'nodes' response is a string with fixed length contacts concatenated arbitrarily.
func parseNodesString(nodes string) (parsed map[string]string) {
	parsed = make(map[string]string)
	if len(nodes)%nodeContactLen > 0 {
		l4g.Info("DHT: Invalid length of nodes.")
		l4g.Info("DHT: Should be a multiple of %d, got %d", nodeContactLen, len(nodes))
		return
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
	r.lastQueryID = (r.lastQueryID + 1) % 256
	transId = strconv.Itoa(r.lastQueryID)
	r.pendingQueries[transId] = &queryType{Type: transType}
	return
}

func (r *remoteNode) wasContactedRecently(ih InfoHash) bool {
	for _, q := range r.pastQueries {
		if q.ih == ih {
			ago := time.Now().Sub(r.lastTime)
			if ago < getPeersRetryPeriod {
				return true
			}
		}
	}
	return false
}

type getPeersResponse struct {
	// TODO: argh, values can be a string depending on the client (e.g: original bittorrent).
	Values []string "values"
	Id     string   "id"
	Nodes  string   "nodes"
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
func sendMsg(conn *net.UDPConn, raddr *net.UDPAddr, query interface{}) {
	totalSent.Add(1)
	var b bytes.Buffer
	if err := bencode.Marshal(&b, query); err != nil {
		return
	}
	if _, err := conn.WriteToUDP(b.Bytes(), raddr); err != nil {
		// debug.Println("DHT: node write failed:", err)
	}
	return
}

// Read responses from bencode-speaking nodes. Return the appropriate data structure.
func readResponse(p packetType) (response responseType, err error) {
	// The calls to bencode.Unmarshal() can be fragile.
	defer func() {
		if x := recover(); x != nil {
			// debug.Printf("DHT: !!! Recovering from panic() after bencode.Unmarshal %q, %v", string(p.b), x)
		}
	}()
	if e2 := bencode.Unmarshal(bytes.NewBuffer(p.b), &response); e2 == nil {
		err = nil
		return
	} else {
		// debug.Printf("DHT: unmarshal error, odd or partial data during UDP read? %v, err=%s", string(p.b), e2)
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
	raddr *net.UDPAddr
}

func listen(listenPort int) (socket *net.UDPConn, err error) {
	// debug.Printf("DHT: Listening for peers on port: %d\n", listenPort)
	listener, err := net.ListenPacket("udp4", ":"+strconv.Itoa(listenPort))
	if err != nil {
		// debug.Println("DHT: Listen failed:", err)
	}
	if listener != nil {
		socket = listener.(*net.UDPConn)
	}
	return
}

// Read from UDP socket, writes slice of byte into channel.
func readFromSocket(socket *net.UDPConn, conChan chan packetType, bytesArena arena) {
	for {
		b := bytesArena.Pop()
		n, addr, err := socket.ReadFromUDP(b)
		b = b[0:n]
		if n == maxUDPPacketSize {
			// debug.Printf("DHT: Warning. Received packet with len >= %d, some data may have been discarded.\n", maxUDPPacketSize)
		}
		if n > 0 && err == nil {
			p := packetType{b, addr}
			conChan <- p
			continue
		}
		// debug.Println("DHT: readResponse error:", err)
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
