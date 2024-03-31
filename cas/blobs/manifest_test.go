package blobs_test

import (
	"lifs_go/cas/blobs"
	"testing"
)

func checkEqual(t *testing.T, m1 *blobs.Manifest, m2 *blobs.Manifest) {
	if m1.Type != m2.Type {
		t.Errorf("type mismatch: %v != %v", m1.Type, m2.Type)
	}
	if m1.Fanout != m2.Fanout {
		t.Errorf("fanout mismatch: %v != %v", m1.Fanout, m2.Fanout)
	}
	if m1.Size != m2.Size {
		t.Errorf("size mismatch: %v != %v", m1.Size, m2.Size)
	}
	if m1.ChunkSize != m2.ChunkSize {
		t.Errorf("chunk size mismatch: %v != %v", m1.ChunkSize, m2.ChunkSize)
	}
	if m1.Root != m2.Root {
		t.Errorf("root mismatch: %v != %v", m1.Root, m2.Root)
	}
}

func TestMarshal(t *testing.T) {
	m := blobs.EmptyManifest("type")
	data, err := blobs.Marshal(m)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	m_, err := blobs.Unmarshal(data)
	if err != nil {
		t.Errorf("unmarshal error: %v", err)
	}
	checkEqual(t, m, m_)
}

func TestMarshalSome(t *testing.T) {
	m1 := blobs.EmptyManifest("type1")
	m2 := blobs.EmptyManifest("type2")
	data, err := blobs.MarshalSome([]*blobs.Manifest{m1, m2})
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	ms, err := blobs.UnmarshalSome(data)
	if err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if len(ms) != 2 {
		t.Errorf("length mismatch: %v != %v", 2, len(ms))
	}
	checkEqual(t, m1, ms[0])
	checkEqual(t, m2, ms[1])
}
