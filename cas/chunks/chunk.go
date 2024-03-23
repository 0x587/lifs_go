package chunks

import (
	"fmt"
	"github.com/codahale/blake2"
	"lifs_go/cas"
)

type Chunk struct {
	Type  string
	Level uint8
	Buf   []byte
}

func (c *Chunk) String() string {
	return fmt.Sprintf("Chunk{%q@%d %x}", c.Type, c.Level, c.Buf)
}

// Copyright notice: this file from https://github.com/bazil/bazil/blob/master/cas/chunks/chunkutil/hash.go

func MakeChunk(typ string, level uint8, data []byte) *Chunk {
	chunk := &Chunk{
		Type:  typ,
		Level: level,
		Buf:   data,
	}
	return chunk
}

const personalizationPrefix = "lifs:"

func Hash(chunk *Chunk) cas.Key {
	var per [blake2.PersonalSize]byte
	copy(per[:], personalizationPrefix)
	copy(per[len(personalizationPrefix):], chunk.Type)
	config := &blake2.Config{
		Size:     cas.KeySize,
		Personal: per[:],
		Tree: &blake2.Tree{
			// We are faking tree mode without any intent to actually
			// follow all the rules, to be able to feed the level
			// into the hash function. These settings are dubious, but
			// we need to do something to make having Tree legal.
			Fanout:        0,
			MaxDepth:      255,
			InnerHashSize: cas.KeySize,

			NodeDepth: chunk.Level,
		},
	}
	h := blake2.New(config)
	if len(chunk.Buf) == 0 {
		return cas.Empty
	}
	_, _ = h.Write(chunk.Buf)
	keyBuf := h.Sum(nil)
	return cas.NewKey(keyBuf)
}
