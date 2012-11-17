package dht

import (
	"code.google.com/p/vitess/go/cache"
	"container/ring"
)

const (
	// Values "inspired" by jch's dht.c.
	maxInfoHashes    = 16384
	maxInfoHashPeers = 2048
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
	if p.Size() > maxInfoHashPeers {
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
		infoHashPeers:        cache.NewLRUCache(maxInfoHashes),
		localActiveDownloads: make(map[string]bool),
	}
}

type peerStore struct {
	// cache of peers for infohashes. Each key is an infohash and the
	// values are peerContactsSet.
	infoHashPeers *cache.LRUCache
	// infoHashes for which we are peers.
	localActiveDownloads map[string]bool
}

func (h *peerStore) size() int {
	length, _, _, _ := h.infoHashPeers.Stats()
	return int(length)
}

func (h *peerStore) get(ih string) *peerContactsSet {
	c, ok := h.infoHashPeers.Get(ih)
	if !ok {
		return nil
	}
	contacts := c.(*peerContactsSet)
	return contacts
}

// count shows the number of know peers for the given infohash.
func (h *peerStore) count(ih string) int {
	peers := h.get(ih)
	if peers == nil {
		return 0
	}
	return peers.Size()
}

// peerContacts returns a random set of 8 peers for the ih InfoHash.
func (h *peerStore) peerContacts(ih string) []string {
	peers := h.get(ih)
	if peers == nil {
		return nil
	}
	return peers.next()
}

// updateContact adds peerContact as a peer for the provided ih. Returns true
// if the contact was added, false otherwise (e.g: already present) .
func (h *peerStore) addContact(ih string, peerContact string) bool {
	var peers *peerContactsSet
	p, ok := h.infoHashPeers.Get(ih)
	if ok {
		peers = p.(*peerContactsSet)
	} else {
		if h.size() > maxInfoHashes {
			return false
		}
		peers = &peerContactsSet{set: make(map[string]bool, maxInfoHashes)}
		h.infoHashPeers.Set(ih, peers)
	}
	return peers.put(peerContact)
}

func (h *peerStore) addLocalDownload(ih string) {
	h.localActiveDownloads[ih] = true
}

func (h *peerStore) hasLocalDownload(ih string) bool {
	_, ok := h.localActiveDownloads[ih]
	return ok
}
