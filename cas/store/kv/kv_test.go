package kv_test

import (
	"context"
	"lifs_go/cas/chunks"
	"lifs_go/cas/store"
	"lifs_go/cas/store/kv"
	kvmem "lifs_go/kv/mem"
	"testing"
)

func NewTestTarget() store.IF {
	return kv.New(kvmem.New())
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
	if ks, ts := key.String(), "4077d28a47aab1811f78b740bef641ebe8cbabaefa142f37112d42d8868c2230d7d42a9566142b14ef99a6bc73bd36ebe696490746849f6791827fd4316ee756"; ks != ts {
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
