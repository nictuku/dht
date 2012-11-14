package dht

import (
	"code.google.com/p/vitess/go/cache"
)

const (
	// Values "inspired" by jch's dht.c.
	maxInfoHashes    = 16384
	maxInfoHashPeers = 2048
)

// For the inner map, the key address in binary form. value=ignored.
type peerContactsSet map[string]bool

func (p peerContactsSet) Size() int {
	return len(p)
}

func newHashStore() *hashStore {
	return &hashStore{
		infoHashPeers:        cache.NewLRUCache(maxInfoHashes),
		localActiveDownloads: make(peerContactsSet),
	}
}

type hashStore struct {
	// cache of peers for infohashes. Each key is an infohash and the values are peerContactsSet.
	infoHashPeers *cache.LRUCache
	// infoHashes for which we are peers.
	localActiveDownloads map[string]bool
}

func (h *hashStore) size() int {
	length, _, _, _ := h.infoHashPeers.Stats()
	return int(length)
}

func (h *hashStore) get(ih string) peerContactsSet {
	c, ok := h.infoHashPeers.Get(ih)
	if !ok {
		return nil
	}
	contacts := c.(peerContactsSet)
	return contacts
}

// count shows the number of know peers for the given infohash.
func (h *hashStore) count(ih string) int {
	return len(h.get(ih))
}

func (h *hashStore) peerContacts(ih string) []string {
	c := make([]string, 0, kNodes)
	for p, _ := range h.get(ih) {
		c = append(c, p)
	}
	return c
}

// updateContact adds peerContact as a peer for the provided ih. Returns true if the contact was added, false otherwise (e.g: already present) .
func (h *hashStore) addContact(ih string, peerContact string) bool {
	var peers peerContactsSet
	p, ok := h.infoHashPeers.Get(ih)
	if ok {
		peers = p.(peerContactsSet)
	} else {
		if h.size() > maxInfoHashes {
			return false
		}
		peers = peerContactsSet{}
		h.infoHashPeers.Set(ih, peers)
	}
	if len(peers) > maxInfoHashPeers {
		return false
	}
	if p := peers[peerContact]; !p {
		peers[peerContact] = true
		return true
	}
	return false
}

func (h *hashStore) addLocalDownload(ih string) {
	h.localActiveDownloads[ih] = true
}

func (h *hashStore) hasLocalDownload(ih string) bool {
	_, ok := h.localActiveDownloads[ih]
	return ok
}
