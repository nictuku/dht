package dht

// TODO: Cleanup stale peer contacts.

import (
	"code.google.com/p/vitess/go/cache"
	"container/ring"
)

var (
	// The default values were inspired by jch's dht.c. The formula to calculate the memory
	// usage is: MaxInfoHashes*MaxInfoHashPeers*len(peerContact).
	//
	// len(peerContact) is ~6 bytes, so after several days the contact store with the default
	// values should consume 192MB of memory.

	// MaxInfoHashes is the limit of number of infohashes for which we should keep a peer list.
	// If this value and MaxInfoHashPeers are unchanged, after several days the used space in
	// RAM would approach 192MB. Large values help keeping the DHT network healthy. This
	// variable can only be changed before the DHT node is created with NewDHTNode.
	MaxInfoHashes = 16384
	// MaxInfoHashPeers is the limit of number of peers to be tracked for each infohash. One
	// single peer contact typically consumes 6 bytes. This variable can only be changed before
	// the DHT node is created with NewDHTNode.
	MaxInfoHashPeers = 2048
)

// For the inner map, the key address in binary form. value=ignored.
type peerContactsSet struct {
	set map[string]bool
	// Needed to ensure different peers are returned each time.
	ring *ring.Ring
}

// next returns up to 8 peer contacts, if available. Further calls will return a
// different set of contacts, if possible.
func (p *peerContactsSet) next() []string {
	count := kNodes
	if count > p.Size() {
		count = p.Size()
	}
	x := make([]string, 0, count)
	var next *ring.Ring
	for i := 0; i < count; i++ {
		next = p.ring.Next()
		x = append(x, next.Value.(string))
		p.ring = next
	}
	return x
}

func (p *peerContactsSet) put(peerContact string) bool {
	if p.Size() > MaxInfoHashPeers {
		return false
	}
	if ok := p.set[peerContact]; !ok {
		p.set[peerContact] = true

		r := &ring.Ring{Value: peerContact}
		if p.ring == nil {
			p.ring = r
		} else {
			p.ring.Link(r)
		}
		return true
	}
	return false
}

func (p *peerContactsSet) Size() int {
	return len(p.set)
}

func newPeerStore() *peerStore {
	return &peerStore{
		infoHashPeers:        cache.NewLRUCache(uint64(MaxInfoHashes)),
		localActiveDownloads: make(map[InfoHash]bool),
	}
}

type peerStore struct {
	// cache of peers for infohashes. Each key is an infohash and the
	// values are peerContactsSet.
	infoHashPeers *cache.LRUCache
	// infoHashes for which we are peers.
	localActiveDownloads map[InfoHash]bool
}

func (h *peerStore) size() int {
	length, _, _, _ := h.infoHashPeers.Stats()
	return int(length)
}

func (h *peerStore) get(ih InfoHash) *peerContactsSet {
	c, ok := h.infoHashPeers.Get(string(ih))
	if !ok {
		return nil
	}
	contacts := c.(*peerContactsSet)
	return contacts
}

// count shows the number of know peers for the given infohash.
func (h *peerStore) count(ih InfoHash) int {
	peers := h.get(ih)
	if peers == nil {
		return 0
	}
	return peers.Size()
}

// peerContacts returns a random set of 8 peers for the ih InfoHash.
func (h *peerStore) peerContacts(ih InfoHash) []string {
	peers := h.get(ih)
	if peers == nil {
		return nil
	}
	return peers.next()
}

// updateContact adds peerContact as a peer for the provided ih. Returns true
// if the contact was added, false otherwise (e.g: already present) .
func (h *peerStore) addContact(ih InfoHash, peerContact string) bool {
	var peers *peerContactsSet
	p, ok := h.infoHashPeers.Get(string(ih))
	if ok {
		var okType bool
		peers, okType = p.(*peerContactsSet)
		if okType && peers != nil {
			return peers.put(peerContact)
		}
	}
	if h.size() > MaxInfoHashes {
		// Already tracking too many infohashes. Drop this insertion.
		return false
	}
	peers = &peerContactsSet{set: make(map[string]bool)}
	h.infoHashPeers.Set(string(ih), peers)
	return peers.put(peerContact)
}

func (h *peerStore) addLocalDownload(ih InfoHash) {
	h.localActiveDownloads[ih] = true
}

func (h *peerStore) hasLocalDownload(ih InfoHash) bool {
	_, ok := h.localActiveDownloads[ih]
	return ok
}
