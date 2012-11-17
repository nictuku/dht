package dht

import (
	"testing"
)

func TestDecodeInfoHash(t *testing.T) {
	infoHash, err := DecodeInfoHash("d1c5676ae7ac98e8b19f63565905105e3c4c37a2")
	if err != nil {
		t.Fatalf("DecodeInfoHash faiure: %v", err)
	}
	if infoHash != "\xd1\xc5\x67\x6a\xe7\xac\x98\xe8\xb1\x9f\x63\x56\x59\x05\x10\x5e\x3c\x4c\x37\xa2" {
		t.Fatalf("unexpected infohash decoding")
	}

}
