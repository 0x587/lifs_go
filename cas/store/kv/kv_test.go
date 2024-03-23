package kv_test

import (
	"context"
	"lifs_go/cas/chunks"
	"lifs_go/cas/store"
	"lifs_go/cas/store/kv"
	kv_ "lifs_go/kv/kvmem"
	"testing"
)

func NewTestTarget() store.Store {
	return kv.NewStoreKV(kv_.NewKvMem())
}

func TestBasic(t *testing.T) {
	value := []byte("value")
	chunk := chunks.MakeChunk("type", 0, value)
	target := NewTestTarget()
	ctx := context.Background()
	key, err := target.Add(ctx, chunk)
	if err != nil {
		t.Fatalf("kvmem.Put fail %v\n", err)
	}
	if ks, ts := key.String(), "277f5df2e35f643bac0a5bff3e3a2d733ae2b78c77ac1f3fff4601f080c1aaf4617879da0262fe08a6925eeaeafe9463fa6927f2dc788c5bbf3e898860986f17"; ks != ts {
		t.Errorf("bad key %s!=%s", ks, ts)
	}
	c, err := target.Get(ctx, key, chunk.Type, chunk.Level)
	if err != nil {
		t.Fatal(err)
	}
	if string(c.Buf) != string(value) {
		t.Errorf("bag Get: %s != %s", c.Buf, value)
	}
}
