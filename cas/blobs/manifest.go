package blobs

import (
	"bytes"
	"encoding/gob"
	"lifs_go/cas"
)

type Manifest struct {
	Type string
	Root cas.Key
	Size uint64
	// Must be >= MinChunkSize.
	ChunkSize uint32
	// Must be >= 2.
	Fanout uint32
}

func Marshal(m *Manifest) ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	err := enc.Encode(m)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func MarshalSome(ms []*Manifest) ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)
	err := enc.Encode(ms)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Unmarshal(data []byte) (*Manifest, error) {
	m := &Manifest{}
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func UnmarshalSome(data []byte) ([]*Manifest, error) {
	var ms []*Manifest
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&ms)
	if err != nil {
		return nil, err
	}
	return ms, nil
}

// EmptyManifest returns an empty manifest of the given type with the
// default tuning parameters.
func EmptyManifest(type_ string) *Manifest {
	const kB = 1024
	const MB = 1024 * kB

	return &Manifest{
		Type:      type_,
		ChunkSize: 4 * MB,
		Fanout:    64,
	}
}
