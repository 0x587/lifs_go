package blobs_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"lifs_go/cas"
	"lifs_go/cas/blobs"
	"lifs_go/cas/store"
	"lifs_go/cas/store/mem"
	"testing"
)

func emptyBlob(t testing.TB, chunkStore store.IF) *blobs.Blob {
	blob, err := blobs.Open(
		chunkStore,
		blobs.EmptyManifest("type_empty"),
	)
	if err != nil {
		t.Fatalf("cannot open blobs: %v", err)
	}
	return blob
}

func TestOpenNoType(t *testing.T) {
	_, err := blobs.Open(mem.New(), &blobs.Manifest{
		// no Type
		ChunkSize: blobs.MinChunkSize,
		Fanout:    2,
	})
	if g, e := err, blobs.ErrMissingType; !errors.Is(g, e) {
		t.Fatalf("bad error: %v != %v", g, e)
	}
}

func TestEmptyRead(t *testing.T) {
	blob := emptyBlob(t, mem.New())
	buf := make([]byte, 10)
	ctx := context.Background()
	n, err := blob.IO(ctx).ReadAt(buf, 3)
	if g, e := err, io.EOF; !errors.Is(g, e) {
		t.Errorf("expected EOF: %v != %v", g, e)
	}
	if g, e := n, 0; g != e {
		t.Errorf("expected to read 0 bytes: %v != %v", g, e)
	}
}

func TestSparseRead(t *testing.T) {
	const chunkSize = 4096
	blob, err := blobs.Open(
		mem.New(),
		&blobs.Manifest{
			Type:      "footype",
			Size:      100,
			ChunkSize: chunkSize,
			Fanout:    2,
		},
	)
	if err != nil {
		t.Fatalf("blobs.Open: %v", err)
	}
	buf := make([]byte, 10)
	ctx := context.Background()
	n, err := blob.IO(ctx).ReadAt(buf, 3)
	if err != nil {
		t.Errorf("unexpected read error: %v", err)
	}
	if g, e := n, 10; g != e {
		t.Errorf("expected to read 0 bytes: %v != %v", g, e)
	}
}

func TestEmptySave(t *testing.T) {
	blob := emptyBlob(t, mem.New())
	ctx := context.Background()
	saved, err := blob.Save(ctx)
	if err != nil {
		t.Errorf("unexpected error from Save: %v", err)
	}
	if g, e := saved.Type, "type_empty"; g != e {
		t.Errorf("unexpected type: %v != %v", g, e)
	}
	if g, e := saved.Root, cas.Empty; g != e {
		t.Errorf("unexpected key: %v != %v", g, e)
	}
	if g, e := saved.Size, uint64(0); g != e {
		t.Errorf("unexpected size: %v != %v", g, e)
	}
}

func TestEmptyDirtySave(t *testing.T) {
	blob := emptyBlob(t, mem.New())
	ctx := context.Background()
	n, err := blob.IO(ctx).WriteAt([]byte{0x00}, 0)
	if err != nil {
		t.Errorf("unexpected error from WriteAt: %v", err)
	}
	if g, e := n, 1; g != e {
		t.Errorf("unexpected write length: %v != %v", g, e)
	}
	if g, e := blob.Size(), uint64(1); g != e {
		t.Errorf("unexpected manifest size: %v != %v", g, e)
	}

	saved, err := blob.Save(ctx)
	if err != nil {
		t.Errorf("unexpected error from Save: %v", err)
	}
	if g, e := saved.Root, cas.Empty; g != e {
		t.Errorf("unexpected key: %v != %v", g, e)
	}
	if g, e := saved.Size, uint64(1); g != e {
		t.Errorf("unexpected size: %v != %v", g, e)
	}
}

var GREETING = []byte("hello, world\n")

func TestWriteAndRead(t *testing.T) {
	blob := emptyBlob(t, mem.New())
	ctx := context.Background()
	n, err := blob.IO(ctx).WriteAt(GREETING, 0)
	if err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if g, e := n, len(GREETING); g != e {
		t.Errorf("unexpected write length: %v != %v", g, e)
	}
	if g, e := blob.Size(), uint64(len(GREETING)); g != e {
		t.Errorf("unexpected manifest size: %v != %v", g, e)
	}

	// do +1 to trigger us seeing EOF too
	buf := make([]byte, len(GREETING)+1)
	n, err = blob.IO(ctx).ReadAt(buf, 0)
	if err != io.EOF {
		t.Errorf("expected read EOF: %v", err)
	}
	if g, e := n, len(GREETING); g != e {
		t.Errorf("unexpected read length: %v != %v", g, e)
	}
	buf = buf[:n]
	if !bytes.Equal(GREETING, buf) {
		t.Errorf("unexpected read data: %q", buf)
	}
}

func TestWriteSaveAndRead(t *testing.T) {
	chunkStore := mem.New()
	ctx := context.Background()
	var saved *blobs.Manifest
	{
		blob := emptyBlob(t, chunkStore)
		n, err := blob.IO(ctx).WriteAt(GREETING, 0)
		if err != nil {
			t.Fatalf("unexpected write error: %v", err)
		}
		if g, e := n, len(GREETING); g != e {
			t.Errorf("unexpected write length: %v != %v", g, e)
		}
		if g, e := blob.Size(), uint64(len(GREETING)); g != e {
			t.Errorf("unexpected manifest size: %v != %v", g, e)
		}
		saved, err = blob.Save(ctx)
		if err != nil {
			t.Fatalf("unexpected error from Save: %v", err)
		}
	}

	b, err := blobs.Open(chunkStore, saved)
	if err != nil {
		t.Fatalf("cannot open saved blob: %v", err)
	}
	// do +1 to trigger us seeing EOF too
	buf := make([]byte, len(GREETING)+1)
	n, err := b.IO(ctx).ReadAt(buf, 0)
	if err != io.EOF {
		t.Errorf("expected read EOF: %v", err)
	}
	if g, e := n, len(GREETING); g != e {
		t.Errorf("unexpected read length: %v != %v", g, e)
	}
	buf = buf[:n]
	if !bytes.Equal(GREETING, buf) {
		t.Errorf("unexpected read data: %q", buf)
	}
}

func TestWriteSaveLoopAndRead(t *testing.T) {
	const chunkSize = 4096
	const fanout = 2
	chunkStore := mem.New()
	blob, err := blobs.Open(chunkStore, &blobs.Manifest{
		Type:      "footype",
		ChunkSize: chunkSize,
		Fanout:    fanout,
	})
	if err != nil {
		t.Fatalf("cannot open blob: %v", err)
	}
	// not exactly sure where this magic number comes from :(
	greeting := bytes.Repeat(GREETING, 40330)

	ctx := context.Background()
	var prev *cas.Key
	for i := 0; i <= 2; i++ {
		n, err := blob.IO(ctx).WriteAt(greeting, 0)
		if err != nil {
			t.Fatalf("unexpected write error: %v", err)
		}
		if g, e := n, len(greeting); g != e {
			t.Errorf("unexpected write length: %v != %v", g, e)
		}
		if g, e := blob.Size(), uint64(len(greeting)); g != e {
			t.Errorf("unexpected manifest size: %v != %v", g, e)
		}
		ctx := context.Background()
		saved, err := blob.Save(ctx)
		if err != nil {
			t.Fatalf("unexpected error from Save: %v", err)
		}
		t.Logf("saved %v size=%d", saved.Root, saved.Size)
		if prev != nil {
			if g, e := saved.Root, *prev; g != e {
				t.Errorf("unexpected key: %q != %q", g, e)
			}
		}
		tmp := saved.Root
		prev = &tmp
	}

	// do +1 to trigger us seeing EOF too
	buf := make([]byte, len(greeting)+1)
	n, err := blob.IO(ctx).ReadAt(buf, 0)
	if err != io.EOF {
		t.Errorf("expected read EOF: %v", err)
	}
	if g, e := n, len(greeting); g != e {
		t.Errorf("unexpected read length: %v != %v", g, e)
	}
	buf = buf[:n]
	if !bytes.Equal(greeting, buf) {
		// assumes len > 100, which we know is true
		t.Errorf("unexpected read data %q..%q", buf[:100], buf[len(buf)-100:])
	}
}

func TestWriteSaveAndReadLarge(t *testing.T) {
	const chunkSize = 4096
	const fanout = 2
	chunkStore := mem.New()
	// just enough to span multiple chunks
	greeting := bytes.Repeat(GREETING, chunkSize/len(GREETING)+1)

	ctx := context.Background()
	var saved *blobs.Manifest
	{
		blob, err := blobs.Open(chunkStore, &blobs.Manifest{
			Type:      "footype",
			ChunkSize: chunkSize,
			Fanout:    fanout,
		})
		if err != nil {
			t.Fatalf("cannot open blob: %v", err)
		}
		n, err := blob.IO(ctx).WriteAt(greeting, 0)
		if err != nil {
			t.Fatalf("unexpected write error: %v", err)
		}
		if g, e := n, len(greeting); g != e {
			t.Errorf("unexpected write length: %v != %v", g, e)
		}
		if g, e := blob.Size(), uint64(len(greeting)); g != e {
			t.Errorf("unexpected manifest size: %v != %v", g, e)
		}
		ctx := context.Background()
		saved, err = blob.Save(ctx)
		if err != nil {
			t.Fatalf("unexpected error from Save: %v", err)
		}
	}

	t.Logf("saved manifest: %+v", saved)
	b, err := blobs.Open(chunkStore, saved)
	if err != nil {
		t.Fatalf("cannot open saved blob: %v", err)
	}
	// do +1 to trigger us seeing EOF too
	buf := make([]byte, len(greeting)+1)
	n, err := b.IO(ctx).ReadAt(buf, 0)
	if err != io.EOF {
		t.Errorf("expected read EOF: %v", err)
	}
	if g, e := n, len(greeting); g != e {
		t.Errorf("unexpected read length: %v != %v", g, e)
	}
	buf = buf[:n]
	if !bytes.Equal(greeting, buf) {
		t.Errorf("unexpected read data: %q", buf)
	}
}

func TestWriteSparse(t *testing.T) {
	const chunkSize = 4096
	chunkStore := mem.New()
	blob, err := blobs.Open(chunkStore, &blobs.Manifest{
		Type:      "footype",
		ChunkSize: chunkSize,
		Fanout:    2,
	})
	if err != nil {
		t.Fatalf("cannot open blob: %v", err)
	}

	ctx := context.Background()
	// note: gap after end of first chunk
	n, err := blob.IO(ctx).WriteAt([]byte{'x'}, chunkSize+3)
	if err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if g, e := n, 1; g != e {
		t.Errorf("unexpected write length: %v != %v", g, e)
	}
	if g, e := blob.Size(), uint64(chunkSize)+3+1; g != e {
		t.Errorf("unexpected manifest size: %v != %v", g, e)
	}

	// read exactly a chunksize to access only the hole
	buf := make([]byte, 1)
	n, err = blob.IO(ctx).ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if g, e := n, len(buf); g != e {
		t.Errorf("unexpected read length: %v != %v", g, e)
	}
	buf = buf[:n]
	if !bytes.Equal([]byte{0}, buf) {
		t.Errorf("unexpected read data: %q", buf)
	}
}

func TestWriteSparseBoundary(t *testing.T) {
	const chunkSize = 4096
	chunkStore := mem.New()
	blob, err := blobs.Open(chunkStore, &blobs.Manifest{
		Type:      "footype",
		ChunkSize: chunkSize,
		Fanout:    2,
	})
	if err != nil {
		t.Fatalf("cannot open blob: %v", err)
	}

	ctx := context.Background()
	n, err := blob.IO(ctx).WriteAt([]byte{'x', 'y'}, chunkSize)
	if err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if g, e := n, 2; g != e {
		t.Errorf("unexpected write length: %v != %v", g, e)
	}
	if g, e := blob.Size(), uint64(chunkSize)+2; g != e {
		t.Errorf("unexpected manifest size: %v != %v", g, e)
	}

	// access only the hole
	buf := make([]byte, 1)
	n, err = blob.IO(ctx).ReadAt(buf, chunkSize)
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if g, e := n, len(buf); g != e {
		t.Errorf("unexpected read length: %v != %v", g, e)
	}
	buf = buf[:n]
	if !bytes.Equal([]byte{'x'}, buf) {
		t.Errorf("unexpected read data: %q", buf)
	}
}

func TestWriteAndSave(t *testing.T) {
	chunkStore := mem.New()
	blob := emptyBlob(t, chunkStore)

	ctx := context.Background()
	n, err := blob.IO(ctx).WriteAt(GREETING, 0)
	if err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if g, e := n, len(GREETING); g != e {
		t.Errorf("unexpected write length: %v != %v", g, e)
	}

	saved, err := blob.Save(ctx)
	if err != nil {
		t.Fatalf("unexpected error from Save: %v", err)
	}
	if g, e := saved.Root.String(), "7c85585d46153d26c4b3ccdc28dc776ce2b7a6350dba2c7ca97b7f315f600caef71b8e9b0b781d539e65cacfa8bdba05f2532cbf0722813bc2e83dae689b5d4d"; g != e {
		t.Errorf("unexpected key: %q != %q", g, e)
	}
	if g, e := saved.Size, uint64(len(GREETING)); g != e {
		t.Errorf("unexpected size: %v != %v", g, e)
	}
}

func TestWriteAndSaveLarge(t *testing.T) {
	const chunkSize = 4096
	const fanout = 64
	chunkStore := mem.New()
	blob, err := blobs.Open(chunkStore, &blobs.Manifest{
		Type:      "footype",
		ChunkSize: chunkSize,
		Fanout:    fanout,
	})
	if err != nil {
		t.Fatalf("cannot open blob: %v", err)
	}
	ctx := context.Background()
	n, err := blob.IO(ctx).WriteAt(bytes.Join([][]byte{
		bytes.Repeat([]byte{'x'}, chunkSize),
		bytes.Repeat([]byte{'y'}, chunkSize),
	}, []byte{}), 0)
	if err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if g, e := n, 2*chunkSize; g != e {
		t.Errorf("unexpected write length: %v != %v", g, e)
	}

	saved, err := blob.Save(ctx)
	if err != nil {
		t.Fatalf("unexpected error from Save: %v", err)
	}
	if g, e := saved.Root.String(), "c66d998cd85de065f9bd9a048bf3a985350cf8d34f389e2020081b0ad81e14a6281d25cd19b7dfec6f0661ec4603cccddfb43dde98d6515d95bded8fa51aa40f"; g != e {
		t.Errorf("unexpected key: %q != %q", g, e)
	}
	if g, e := saved.Size, uint64(chunkSize+chunkSize); g != e {
		t.Errorf("unexpected size: %v != %v", g, e)
	}
}

func TestWriteTruncateZero(t *testing.T) {
	const chunkSize = 4096
	const fanout = 64
	blob, err := blobs.Open(mem.New(), &blobs.Manifest{
		Type:      "footype",
		ChunkSize: chunkSize,
		Fanout:    fanout,
	})
	if err != nil {
		t.Fatalf("cannot open blob: %v", err)
	}

	ctx := context.Background()
	n, err := blob.IO(ctx).WriteAt(bytes.Join([][]byte{
		bytes.Repeat([]byte{'x'}, chunkSize),
		bytes.Repeat([]byte{'y'}, chunkSize),
	}, []byte{}), 0)
	if err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if g, e := n, 2*chunkSize; g != e {
		t.Errorf("unexpected write length: %v != %v", g, e)
	}

	_, err = blob.Save(ctx)
	if err != nil {
		t.Fatalf("unexpected error from Save: %v", err)
	}

	err = blob.Truncate(ctx, 0)
	if err != nil {
		t.Fatalf("unexpected Truncate error: %v", err)
	}

	if g, e := blob.Size(), uint64(0); g != e {
		t.Errorf("unexpected manifest size: %v != %v", g, e)
	}

	saved, err := blob.Save(ctx)
	if err != nil {
		t.Errorf("unexpected error from Save: %v", err)
	}
	if g, e := saved.Root, cas.Empty; g != e {
		t.Errorf("unexpected key: %v != %v", g, e)
	}
	if g, e := saved.Size, uint64(0); g != e {
		t.Errorf("unexpected size: %v != %v", g, e)
	}
}

func TestWriteTruncateShrink(t *testing.T) {
	const chunkSize = 4096
	const fanout = 64
	chunkStore := mem.New()
	blob, err := blobs.Open(chunkStore, &blobs.Manifest{
		Type:      "footype",
		ChunkSize: chunkSize,
		Fanout:    fanout,
	})
	if err != nil {
		t.Fatalf("cannot open blob: %v", err)
	}

	ctx := context.Background()
	n, err := blob.IO(ctx).WriteAt(bytes.Join([][]byte{
		bytes.Repeat([]byte{'x'}, chunkSize),
		bytes.Repeat([]byte{'y'}, chunkSize),
	}, []byte{}), 0)
	if err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if g, e := n, 2*chunkSize; g != e {
		t.Errorf("unexpected write length: %v != %v", g, e)
	}

	_, err = blob.Save(ctx)
	if err != nil {
		t.Fatalf("unexpected error from Save: %v", err)
	}

	// shrink enough to need less depth in tree
	const newSize = 5
	err = blob.Truncate(ctx, newSize)
	if err != nil {
		t.Fatalf("unexpected Truncate error: %v", err)
	}

	if g, e := blob.Size(), uint64(newSize); g != e {
		t.Errorf("unexpected manifest size: %v != %v", g, e)
	}

	// do +1 to trigger us seeing EOF too
	buf := make([]byte, newSize+1)
	n, err = blob.IO(ctx).ReadAt(buf, 0)
	if err != io.EOF {
		t.Errorf("expected read EOF: %v", err)
	}
	if g, e := n, newSize; g != e {
		t.Errorf("unexpected read length: %v != %v", g, e)
	}
	buf = buf[:n]
	if g, e := buf, []byte("xxxxx"); !bytes.Equal(g, e) {
		t.Errorf("unexpected read data: %q != %q", g, e)
	}

	saved, err := blob.Save(ctx)
	if err != nil {
		t.Fatalf("unexpected error from Save: %v", err)
	}
	if g, e := saved.Size, uint64(newSize); g != e {
		t.Errorf("unexpected size: %v != %v", g, e)
	}
	{
		blob, err := blobs.Open(chunkStore, saved)
		if err != nil {
			t.Fatalf("cannot open saved blob: %v", err)
		}
		buf := make([]byte, newSize+1)
		n, err = blob.IO(ctx).ReadAt(buf, 0)
		if err != io.EOF {
			t.Errorf("expected read EOF: %v", err)
		}
		if g, e := n, newSize; g != e {
			t.Errorf("unexpected read length: %v != %v", g, e)
		}
		buf = buf[:n]
		if g, e := buf, []byte("xxxxx"); !bytes.Equal(g, e) {
			t.Errorf("unexpected read data: %q != %q", g, e)
		}
	}
}

func TestWriteTruncateGrow(t *testing.T) {
	const chunkSize = 4096
	const fanout = 64
	chunkStore := mem.New()
	blob, err := blobs.Open(chunkStore, &blobs.Manifest{
		Type:      "footype",
		ChunkSize: chunkSize,
		Fanout:    fanout,
	})
	if err != nil {
		t.Fatalf("cannot open blob: %v", err)
	}

	ctx := context.Background()
	n, err := blob.IO(ctx).WriteAt(GREETING, 0)
	if err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if g, e := n, len(GREETING); g != e {
		t.Errorf("unexpected write length: %v != %v", g, e)
	}
	if g, e := blob.Size(), uint64(len(GREETING)); g != e {
		t.Errorf("unexpected manifest size: %v != %v", g, e)
	}

	_, err = blob.Save(ctx)
	if err != nil {
		t.Fatalf("unexpected error from Save: %v", err)
	}

	// grow enough to need a new chunk
	const newSize = chunkSize + 3
	err = blob.Truncate(ctx, newSize)
	if err != nil {
		t.Fatalf("unexpected Truncate error: %v", err)
	}

	if g, e := blob.Size(), uint64(newSize); g != e {
		t.Errorf("unexpected manifest size: %v != %v", g, e)
	}

	// do +1 to trigger us seeing EOF too
	buf := make([]byte, newSize+1)
	n, err = blob.IO(ctx).ReadAt(buf, 0)
	if err != io.EOF {
		t.Errorf("expected read EOF: %v", err)
	}
	if g, e := n, newSize; g != e {
		t.Errorf("unexpected read length: %v != %v", g, e)
	}
	buf = buf[:n]
	want := bytes.Join([][]byte{
		GREETING,
		make([]byte, newSize-len(GREETING)),
	}, []byte{})
	if g, e := buf, want; !bytes.Equal(g, e) {
		t.Errorf("unexpected read data: %q != %q", g, e)
	}

	saved, err := blob.Save(ctx)
	if err != nil {
		t.Fatalf("unexpected error from Save: %v", err)
	}
	if g, e := saved.Size, uint64(newSize); g != e {
		t.Errorf("unexpected size: %v != %v", g, e)
	}
	{
		blob, err := blobs.Open(chunkStore, saved)
		if err != nil {
			t.Fatalf("cannot open saved blob: %v", err)
		}
		buf := make([]byte, newSize+1)
		n, err = blob.IO(ctx).ReadAt(buf, 0)
		if err != io.EOF {
			t.Errorf("expected read EOF: %v", err)
		}
		if g, e := n, newSize; g != e {
			t.Errorf("unexpected read length: %v != %v", g, e)
		}
		buf = buf[:n]
		want := bytes.Join([][]byte{
			GREETING,
			make([]byte, newSize-len(GREETING)),
		}, []byte{})
		if g, e := buf, want; !bytes.Equal(g, e) {
			t.Errorf("unexpected read data: %q != %q", g, e)
		}
	}
}

func BenchmarkWriteSmall(b *testing.B) {
	blob := emptyBlob(b, mem.New())
	ctx := context.Background()
	bio := blob.IO(ctx)

	b.SetBytes(int64(len(GREETING)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bio.WriteAt(GREETING, 0)
		if err != nil {
			b.Fatalf("unexpected write error: %v", err)
		}
		_, err = blob.Save(ctx)
		if err != nil {
			b.Fatalf("unexpected error from Save: %v", err)
		}
	}
}

func BenchmarkWriteBig(b *testing.B) {
	body := bytes.Repeat(GREETING, 1000000)
	blob := emptyBlob(b, mem.New())
	ctx := context.Background()
	bio := blob.IO(ctx)

	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := bio.WriteAt(body, 0)
		if err != nil {
			b.Fatalf("unexpected write error: %v", err)
		}
		_, err = blob.Save(ctx)
		if err != nil {
			b.Fatalf("unexpected error from Save: %v", err)
		}
	}
}
