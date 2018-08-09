package dht

import (
	"testing"
)

func TestPeerStorage(t *testing.T) {
	ih, err := DecodeInfoHash("c3c5fe05c329ae51c6eca464f6b30ba0a457b2ca")
	if err != nil {
		t.Fatalf("DecodeInfoHash: %v", err)
	}
	// Allow 1 IH and 2 peers.
	p := newPeerStore(1, 2, 0)

	if ok := p.addContact(ih, "abcedf"); !ok {
		t.Fatalf("addContact(1/2) expected true, got false")
	}
	if p.count(ih) != 1 {
		t.Fatalf("Added 1st contact, got count %v, wanted 1", p.count(ih))
	}
	p.addContact(ih, "ABCDEF")
	if p.count(ih) != 2 {
		t.Fatalf("Added 2nd contact, got count %v, wanted 2", p.count(ih))
	}
	p.addContact(ih, "ABCDEF")
	if p.count(ih) != 2 {
		t.Fatalf("Repeated 2nd contact, got count %v, wanted 2", p.count(ih))
	}
	p.addContact(ih, "XXXXXX")
	if p.count(ih) != 2 {
		t.Fatalf("Added 3rd contact, got count %v, wanted 2", p.count(ih))
	}

	ih2, err := DecodeInfoHash("e84213a794f3ccd890382a54a64ca68b7e925433")
	if err != nil {
		t.Fatalf("DecodeInfoHash: %v", err)
	}
	if p.count(ih2) != 0 {
		t.Fatalf("ih2 got count %d, wanted 0", p.count(ih2))
	}
	p.addContact(ih2, "ABCDEF")
	if p.count(ih) != 0 {
		t.Fatalf("ih got count %d, wanted 0", p.count(ih))
	}
	if p.count(ih2) != 1 {
		t.Fatalf("ih2 got count %d, wanted 1", p.count(ih))
	}
}
