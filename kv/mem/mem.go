package mem

import (
	"context"
	"lifs_go/kv"
)

type Impl struct {
	data map[string][]byte
}

func (m *Impl) Get(ctx context.Context, key []byte) ([]byte, error) {
	v, found := m.data[string(key)]
	if !found {
		return nil, kv.NotFoundError{key}
	}
	return v, nil
}

func (m *Impl) Put(ctx context.Context, key, value []byte) error {
	if m.data == nil {
		m.data = make(map[string][]byte)
	}
	m.data[string(key)] = value
	return nil
}

func New() kv.IF {
	return &Impl{}
}
