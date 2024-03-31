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
	if g, e := k.String(), "b1de301ec218d8649586a4e474e4c44d26b8dbc3bdcb7ea24fbf0634956469c0e17008af3186c4a4daf566e26b865a00cd3cc0fc34bcc736b569ccf5dfc59acf"; g != e {
		t.Errorf("wrong key for some zero bytes: %v != %v", g, e)
	}
}
