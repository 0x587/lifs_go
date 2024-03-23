package kvmem

import (
	"context"
	"lifs_go/kv"
)

type KvMem struct {
	data map[string][]byte
}

var _ kv.KV = (*KvMem)(nil)

func (m *KvMem) Get(ctx context.Context, key []byte) ([]byte, error) {
	v, found := m.data[string(key)]
	if !found {
		return nil, kv.NotFoundError{key}
	}
	return v, nil
}

func (m *KvMem) Put(ctx context.Context, key, value []byte) error {
	if m.data == nil {
		m.data = make(map[string][]byte)
	}
	m.data[string(key)] = value
	return nil
}

func NewKvMem() *KvMem {
	return &KvMem{}
}
