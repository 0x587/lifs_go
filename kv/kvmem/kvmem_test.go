package kvmem_test

import (
	"context"
	"errors"
	"fmt"
	"lifs_go/kv"
	"lifs_go/kv/kvmem"
	"strings"
	"testing"
)

func NewTestTarget() kv.KV {
	return kvmem.NewKvMem()
}

func TestPut(t *testing.T) {
	key := []byte("key")
	value := []byte("value")
	target := NewTestTarget()
	ctx := context.Background()
	if err := target.Put(ctx, key, value); err != nil {
		t.Fatalf("kvmem.Put fail %v\n", err)
	}
}

func TestGet(t *testing.T) {
	key := []byte("key")
	value := []byte("value")
	target := NewTestTarget()
	ctx := context.Background()
	if err := target.Put(ctx, key, value); err != nil {
		t.Fatalf("kvmem.Put fail %v\n", err)
	}

	v, err := target.Get(ctx, key)
	if err != nil {
		t.Fatalf("kvmem.Get fail %v\n", err)
	}
	if string(v) != string(value) {
		t.Fatalf("kvmem.Get gave wrong content: %q != %q", v, value)
	}
}

func TestPutOverwrite(t *testing.T) {
	key := []byte("key")
	value := []byte("value")
	otherValue := []byte("otherValue")
	target := NewTestTarget()
	ctx := context.Background()
	if err := target.Put(ctx, key, value); err != nil {
		t.Fatalf("kvmem.Put fail %v\n", err)
	}

	v, err := target.Get(ctx, key)
	if err != nil {
		t.Fatalf("kvmem.Get fail %v\n", err)
	}
	if string(v) != string(value) {
		t.Fatalf("kvmem.Get gave wrong content: %q != %q", v, value)
	}

	if err := target.Put(ctx, key, otherValue); err != nil {
		t.Fatalf("kvmem.Put fail %v\n", err)
	}

	v, err = target.Get(ctx, key)
	if err != nil {
		t.Fatalf("kvmem.Get fail %v\n", err)
	}
	if string(v) != string(otherValue) {
		t.Fatalf("kvmem.Get gave wrong content: %q != %q", v, value)
	}
}

func TestGetNotFoundError(t *testing.T) {
	target := NewTestTarget()
	ctx := context.Background()

	const KEY = "this is a wrong key"
	_, err := target.Get(ctx, []byte(KEY))
	if err == nil {
		t.Fatalf("kvmem.Get should have failed")
	}
	var nf kv.NotFoundError
	ok := errors.As(err, &nf)
	if !ok {
		t.Fatalf("kvmem.Get error is of wrong type: %T: %v", err, err)
	}

	if g, w := string(nf.Key), KEY; g != w {
		t.Errorf("NotFoundError Key is wrong: %x != %x", g, w)
	}

	if !strings.Contains(nf.Error(), fmt.Sprintf("%x", KEY)) {
		t.Errorf("NotFoundError not contain Key: (%x) [NotFoundError is: %s]", KEY, nf.Error())
	}
}
