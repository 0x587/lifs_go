package kv

import (
	"context"
	"lifs_go/cas"
	"lifs_go/cas/chunks"
	"lifs_go/cas/store"
	"lifs_go/kv"
)

type KV struct {
	kv kv.KV
}

var _ store.Store = (*KV)(nil)

func makeKey(key cas.Key, typ string, level uint8) []byte {
	k := make([]byte, 0, cas.KeySize+len(typ)+1)
	k = append(k, key.Bytes()...)
	k = append(k, typ...)
	k = append(k, level)
	return k
}

func (k *KV) get(ctx context.Context, key cas.Key, type_ string, level uint8) ([]byte, error) {
	return k.kv.Get(ctx, makeKey(key, type_, level))
}

func (k *KV) Get(ctx context.Context, key cas.Key, type_ string, level uint8) (*chunks.Chunk, error) {
	return store.HandleGet(ctx, k.get, key, type_, level)
}

func (k *KV) Add(ctx context.Context, chunk *chunks.Chunk) (key cas.Key, err error) {
	key = chunks.Hash(chunk)
	if key.IsSpecial() {
		return key, nil
	}
	key_ := makeKey(key, chunk.Type, chunk.Level)
	if err := k.kv.Put(ctx, key_, chunk.Buf); err != nil {
		return cas.Invalid, err
	}
	return key, nil
}

func NewStoreKV(kv kv.KV) store.Store {
	return &KV{kv: kv}
}
