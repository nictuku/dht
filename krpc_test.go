package dht

import (
	"testing"
)

func TestDecodeInfoHash(t *testing.T) {
	infoHash, err := DecodeInfoHash("c3c5fe05c329ae51c6eca464f6b30ba0a457b2ca")
	if err != nil {
		t.Fatalf("DecodeInfoHash faiure: %v", err)
	}
	if infoHash != "\xd1\xc5\x67\x6a\xe7\xac\x98\xe8\xb1\x9f\x63\x56\x59\x05\x10\x5e\x3c\x4c\x37\xa2" {
		t.Fatalf("unexpected infohash decoding")
	}

}
