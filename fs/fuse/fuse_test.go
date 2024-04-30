package fuse_test

import (
	"bytes"
	"io"
	"lifs_go/cas/store/mem"
	"lifs_go/fs/fuse"
	"os"
	"path"
	"testing"
)

func MountInTemp(t *testing.T) (tmp string, cf func()) {
	tmp, _ = os.MkdirTemp(os.TempDir(), "test-")
	v := fuse.New(mem.New())
	unmountFunc, err := v.Mount(tmp)
	if err != nil {
		t.Fatalf("mount err: %v", err)
	}
	cf = func() {
		_ = os.RemoveAll(tmp)
		unmountFunc()
	}
	return
}

func TestBasic(t *testing.T) {
	tmp, cf := MountInTemp(t)
	defer cf()

	stat, err := os.Stat(tmp)
	if err != nil {
		t.Errorf("get root stat error: %v", err)
	}
	if stat.Mode()&os.ModeType != os.ModeDir {
		t.Errorf("root is not a dir: %#v", stat)
	}
	if stat.Mode().Perm() != 0755 {
		t.Errorf("root has wrong access mode: %v", stat.Mode().Perm())
	}

	rf, err := os.Open(tmp)
	if err != nil {
		t.Fatalf("open root error: %v", err)
	}

	names, err := rf.Readdirnames(5)
	if err != nil && err != io.EOF {
		t.Fatalf("list root error: %v", err)
	}
	if len(names) > 0 {
		t.Errorf("unexpected content in root: %v", names)
	}
	err = rf.Close()
	if err != nil {
		t.Fatalf("close root error:%v", err)
	}
}

func TestCreateFile(t *testing.T) {
	tmp, cf := MountInTemp(t)
	defer cf()

	fp, err := os.Create(path.Join(tmp, "file"))
	if err != nil {
		t.Fatalf("create in root error: %v", err)
	}
	err = fp.Close()
	if err != nil {
		t.Fatalf("close file in root error: %v", err)
	}
}

func TestCreateDir(t *testing.T) {
	tmp, cf := MountInTemp(t)
	defer cf()

	p := path.Join(tmp, "dir")
	err := os.Mkdir(p, 0750)
	if err != nil {
		t.Fatalf("mkdir in root error: %v", err)
	}
	df, err := os.Open(p)
	if err != nil {
		t.Fatalf("open dir error: %v", err)
	}
	stat, err := os.Stat(p)
	if err != nil {
		t.Fatalf("stat dir error: %v", err)
	}
	if stat.Mode()&os.ModeType != os.ModeDir {
		t.Errorf("dir has wrong mode: %v", stat.Mode())
	}
	err = os.Mkdir(path.Join(p, "subdir"), 0750)
	if err != nil {
		t.Fatalf("mkdir in dir error: %v", err)
	}
	names, err := df.Readdirnames(5)
	if err != nil && err != io.EOF {
		t.Fatalf("list dir error: %v", err)
	}
	if len(names) != 1 {
		t.Errorf("expect 1 entry, but got %d", len(names))
	}
	err = df.Close()
	if err != nil {
		t.Fatalf("close dir error: %v", err)
	}
}

func TestReadAndWrite(t *testing.T) {
	tmp, cf := MountInTemp(t)
	defer cf()

	p := path.Join(tmp, "file")
	fp, err := os.Create(p)
	if err != nil {
		t.Fatalf("create file error: %v", err)
	}
	defer func(fp *os.File) {
		err = fp.Close()
		if err != nil {
			t.Fatalf("close file error: %v", err)
		}
	}(fp)

	content := []byte("Hello")
	buf := bytes.Repeat(content, 1024)

	n, err := fp.WriteAt(buf, 0)
	if err != nil {
		t.Fatalf("write error: %v", err)
	}

	fp, err = os.Open(p)
	if err != nil {
		t.Fatalf("open file error: %v", err)
	}
	defer func() {
		err = fp.Close()
		if err != nil {
			t.Fatalf("close file error: %v", err)
		}
	}()

	readBuf := make([]byte, len(buf)+1)
	n, err = fp.ReadAt(readBuf, 0)
	if err != io.EOF {
		t.Fatalf("expect EOF, but got: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("expect read %d"+
			" bytes, but got %d", len(buf), n)
	}
	if !bytes.Equal(buf, readBuf[:len(buf)]) {
		t.Fatalf("read content is not equal to write content")
	}

	// read from offset
	readBuf = make([]byte, len(content))
	n, err = fp.ReadAt(readBuf, int64(len(content)))
	if err != nil {
		t.Fatalf("read from offset error: %v", err)
	}
	if n != len(content) {
		t.Fatalf("expect read %d bytes, but got %d", len(content), n)
	}
	if !bytes.Equal(content, readBuf) {
		t.Fatalf("read content is not equal to write content")
	}
}
