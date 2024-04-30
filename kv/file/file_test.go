package file_test

import (
	"context"
	"errors"
	"lifs_go/kv"
	"lifs_go/kv/file"
	"os"
	"testing"
)

func TestAdd(t *testing.T) {
	temp := os.TempDir()
	k := file.New(temp)

	ctx := context.Background()
	err := k.Put(ctx, []byte("quux"), []byte("foobar"))
	if err != nil {
		t.Fatalf("c.Put fail: %v\n", err)
	}
}

func TestGet(t *testing.T) {
	temp := os.TempDir()
	k := file.New(temp)

	ctx := context.Background()
	err := k.Put(ctx, []byte("quux"), []byte("foobar"))
	if err != nil {
		t.Fatalf("c.Put fail: %v\n", err)
	}

	data, err := k.Get(ctx, []byte("quux"))
	if err != nil {
		t.Fatalf("c.Get failed: %v", err)
	}
	if g, e := string(data), "foobar"; g != e {
		t.Fatalf("c.Get gave wrong content: %q != %q", g, e)
	}
}

func TestPutOverwrite(t *testing.T) {
	temp := os.TempDir()
	k := file.New(temp)

	ctx := context.Background()
	err := k.Put(ctx, []byte("quux"), []byte("foobar"))
	if err != nil {
		t.Fatalf("k.Put fail: %v\n", err)
	}

	err = k.Put(ctx, []byte("quux"), []byte("foobar"))
	if err != nil {
		t.Fatalf("k.Put fail: %v\n", err)
	}
}

func TestGetNotFoundError(t *testing.T) {
	temp := os.TempDir()
	k := file.New(temp)

	const KEY = "\x8d\xf3\x1f\x60\xd6\xae\xab\xd0\x1b\x7d\xc8\x3f\x27\x7d\x0e\x24\xcb\xe1\x04\xf7\x29\x0f\xf8\x90\x77\xa7\xeb\x58\x64\x60\x68\xed\xfe\x1a\x83\x02\x28\x66\xc4\x6f\x65\xfb\x91\x61\x2e\x51\x6e\x0e\xcf\xa5\xcb\x25\xfc\x16\xb3\x7d\x2c\x8d\x73\x73\x2f\xe7\x4c\xb2"
	ctx := context.Background()
	_, err := k.Get(ctx, []byte(KEY))
	if err == nil {
		t.Fatalf("c.Get should have failed")
	}
	var nf kv.NotFoundError
	ok := errors.As(err, &nf)
	if !ok {
		t.Fatalf("c.Get error is of wrong type: %T: %v", err, err)
	}

	if g, w := string(nf.Key), KEY; g != w {
		t.Errorf("NotFoundError Key is wrong: %x != %x", g, w)
	}
}
