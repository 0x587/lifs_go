package chunks_test

import (
	"lifs_go/cas"
	"lifs_go/cas/chunks"
	"testing"
)

func TestHashEmpty(t *testing.T) {
	c := &chunks.Chunk{
		Type:  "testchunk",
		Level: 42,
		Buf:   []byte{},
	}
	k := chunks.Hash(c)
	if g, e := k, cas.Empty; g != e {
		t.Errorf("wrong key for zero chunks: %v != %v", g, e)
	}
}

func TestHashSomeZeroes(t *testing.T) {
	c := &chunks.Chunk{
		Type:  "testchunk",
		Level: 42,
		Buf:   []byte{0x00, 0x00, 0x00},
	}
	k := chunks.Hash(c)
	if g, e := k.String(), "d65b5bf2caf92d88848934da742375621f0f4b2ed6aa03a4256c30a16953467ea984a18bb925d6195e1fe5518eaf906e56142ceba66e1c6606b123eb44d59d40"; g != e {
		t.Errorf("wrong key for some zero bytes: %v != %v", g, e)
	}
}
