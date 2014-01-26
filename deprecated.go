package dht

// DoDHT has been deprecated. Please use Run instead.
func (d *DHT) DoDHT() {
	d.Run()
}

// NewDHTNode has been deprecated. Please use New instead.
func NewDHTNode(port, numTargetPeers int, storeEnabled bool) (node *DHT, err error) {
	return New(port, numTargetPeers, storeEnabled, nil)
}
