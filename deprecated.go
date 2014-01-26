package dht

// DoDHT has been deprecated. Please use Run instead.
func (d *DHT) DoDHT() {
	d.Run()
}

// NewDHTNode has been deprecated. Please use New and NewConfig instead.
func NewDHTNode(port, numTargetPeers int, storeEnabled bool) (node *DHT, err error) {
	cfg := NewConfig()
	cfg.SaveRoutingTable = storeEnabled
	cfg.Port = port
	cfg.NumTargetPeers = numTargetPeers
	return New(cfg)
}
