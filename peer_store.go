package dht

// TODO: Cleanup stale peer contacts.

import (
	"container/ring"

	log "github.com/golang/glog"
	"github.com/golang/groupcache/lru"
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
	if count > len(p.set) {
		count = len(p.set)
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

// put adds a peerContact to an infohash contacts set. peerContact must be a binary encoded contact
// address where the first four bytes form the IP and the last byte is the port. IPv6 addresses are
// not currently supported. peerContact with less than 6 bytes will not be stored.
func (p *peerContactsSet) put(peerContact string) bool {
	if len(peerContact) < 6 {
		return false
	}
	if ok := p.set[peerContact]; ok {
		return false
	}
	p.set[peerContact] = true
	r := &ring.Ring{Value: peerContact}
	if p.ring == nil {
		p.ring = r
	} else {
		p.ring.Link(r)
	}
	return true
}

// Size is the number of contacts known for an infohash.
func (p *peerContactsSet) Size() int {
	return len(p.set)
}

func newPeerStore(maxInfoHashes, maxInfoHashPeers int) *peerStore {
	return &peerStore{
		infoHashPeers:        lru.New(maxInfoHashes),
		localActiveDownloads: make(map[InfoHash]bool),
		maxInfoHashes:        maxInfoHashes,
		maxInfoHashPeers:     maxInfoHashPeers,
	}
}

type peerStore struct {
	// cache of peers for infohashes. Each key is an infohash and the
	// values are peerContactsSet.
	infoHashPeers *lru.Cache
	// infoHashes for which we are peers.
	localActiveDownloads map[InfoHash]bool
	maxInfoHashes        int
	maxInfoHashPeers     int
}

func (h *peerStore) get(ih InfoHash) *peerContactsSet {
	c, ok := h.infoHashPeers.Get(string(ih))
	if !ok {
		return nil
	}
	contacts := c.(*peerContactsSet)
	return contacts
}

// count shows the number of known peers for the given infohash.
func (h *peerStore) count(ih InfoHash) int {
	peers := h.get(ih)
	if peers == nil {
		return 0
	}
	return len(peers.set)
}

// peerContacts returns a random set of 8 peers for the ih InfoHash.
func (h *peerStore) peerContacts(ih InfoHash) []string {
	peers := h.get(ih)
	if peers == nil {
		return nil
	}
	return peers.next()
}

// addContact as a peer for the provided ih. Returns true if the contact was
// added, false otherwise (e.g: already present, or invalid).
func (h *peerStore) addContact(ih InfoHash, peerContact string) bool {
	var peers *peerContactsSet
	p, ok := h.infoHashPeers.Get(string(ih))
	if ok {
		var okType bool
		peers, okType = p.(*peerContactsSet)
		if okType && peers != nil {
			if len(peers.set) >= h.maxInfoHashPeers {
				// Already tracking too many peers for this infohash.
				// TODO: Use a circular buffer and discard
				// other contacts.
				return false
			}
			h.infoHashPeers.Add(string(ih), peers)
			return peers.put(peerContact)
		}
		// Bogus peer contacts, reset them.
	}
	peers = &peerContactsSet{set: make(map[string]bool)}
	h.infoHashPeers.Add(string(ih), peers)
	return peers.put(peerContact)
}

func (h *peerStore) addLocalDownload(ih InfoHash) {
	h.localActiveDownloads[ih] = true
}

func (h *peerStore) hasLocalDownload(ih InfoHash) bool {
	_, ok := h.localActiveDownloads[ih]
	log.V(3).Infof("hasLocalDownload for %x: %v", ih, ok)
	return ok
}
