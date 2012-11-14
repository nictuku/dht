package dht

func newHashStore() *hashStore {
	return &hashStore{
		infoHashPeers:        make(map[string]map[string]bool),
		localActiveDownloads: make(map[string]bool),
	}
}

type hashStore struct {
	// map of infohashes to remote peers.
	// key1 == infoHash, key2 == address in binary form. value=ignored.
	infoHashPeers map[string]map[string]bool
	// infoHashes for which we are peers.
	localActiveDownloads map[string]bool
}

// count shows the number of know peers for the given infohash.
func (h *hashStore) count(ih string) int {
	return len(h.infoHashPeers["ih"])
}

func (h *hashStore) peerContacts(ih string) []string {
	c := make([]string, 0, kNodes)
	for p, _ := range h.infoHashPeers[ih] {
		c = append(c, p)
	}
	return c
}

// updateContact adds peerContact as a peer for the provided ih. Returns true if the contact was added, false otherwise (e.g: already present) .
func (h *hashStore) addContact(ih string, peerContact string) bool {
	peers, ok := h.infoHashPeers[ih]
	if !ok {
		peers = map[string]bool{}
		h.infoHashPeers[ih] = peers
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
