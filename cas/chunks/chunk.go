package chunks

import (
	"fmt"
	"github.com/enceve/crypto/blake2/blake2b"
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

	var per [16]byte
	copy(per[:], personalizationPrefix)
	copy(per[len(personalizationPrefix):], chunk.Type)
	config := &blake2b.Config{
		Key:      per[:],
		Salt:     per[:],
		Personal: per[:],
	}
	h, _ := blake2b.New(cas.KeySize, config)
	if len(chunk.Buf) == 0 {
		return cas.Empty
	}
	_, _ = h.Write(chunk.Buf)
	keyBuf := h.Sum(nil)
	return cas.NewKey(keyBuf)
}
