package dht

import "sync"

// arena is a free list of byte slices. After creation, they can be used by
// calling Pop() but afterwords the caller is responsible for calling Push(),
// which puts the blocks back in the queue for later usage. The bytes given by
// Pop() are *not* zeroed so the caller should only read positions that it has
// written to - that can be done easily by shortening the slice at the right
// point.  written to.
type arena struct {
	// I could have used a buffered channel to implement the freelist, but
	// this is probably faster.
	sync.Mutex
	blocks [][]byte
	bsize  int
}

func newArena(blockSize int, numBlocks int) *arena {
	b := make([][]byte, numBlocks)
	for i, _ := range b {
		b[i] = make([]byte, blockSize)
	}
	return &arena{blocks: b, bsize: blockSize}
}

func (a *arena) Pop() (x []byte) {
	a.Lock()
	defer a.Unlock()
	if len(a.blocks) == 0 {
		panic("arena out of space")
		return make([]byte, a.bsize)
	}
	x, a.blocks = a.blocks[len(a.blocks)-1], a.blocks[:len(a.blocks)-1]
	return x
}

func (a *arena) Push(x []byte) {
	a.Lock()
	x = x[:cap(x)]
	a.blocks = append(a.blocks, x)
	a.Unlock()
}
