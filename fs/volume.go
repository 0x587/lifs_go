package fs

import (
	"context"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"lifs_go/cas/blobs"
	"lifs_go/cas/store"
	"lifs_go/cas/store/mem"
	"syscall"
)

type Volume struct {
	fs.Inode
	s        store.Store
	mkv      map[string]*blobs.Manifest
	files    map[string]*File
	children map[string]*fs.Inode
}

func (v *Volume) getFile(ctx context.Context, name string, m *blobs.Manifest) *File {
	file, ok := v.files[name]
	if ok {
		return file
	}
	file = NewFile(v.s, m)
	v.files[name] = file
	return file
}

func (v *Volume) getChild(ctx context.Context, name string, m *blobs.Manifest) *fs.Inode {
	node, ok := v.children[name]
	if ok {
		return node
	}
	f := v.getFile(ctx, name, m)
	node = v.NewInode(ctx, f, fs.StableAttr{Mode: fuse.S_IFREG})
	v.children[name] = node
	return node
}

func (v *Volume) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (
	node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	m := blobs.EmptyManifest("file")
	v.mkv[name] = m
	return v.getChild(ctx, name, m), v.getFile(ctx, name, m), flags, syscall.F_OK
}

var _ fs.NodeCreater = (*Volume)(nil)

func (v *Volume) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := make([]fuse.DirEntry, 0)
	for name, _ := range v.mkv {
		entries = append(entries, fuse.DirEntry{Mode: fuse.S_IFREG, Name: name})
	}
	res := fs.NewListDirStream(entries)
	return res, syscall.F_OK
}

var _ fs.NodeReaddirer = (*Volume)(nil)

func (v *Volume) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	// for file
	m, ok := v.mkv[name]
	if ok {
		return v.getChild(ctx, name, m), syscall.F_OK
	}
	return nil, syscall.ENOENT
}

var _ fs.NodeLookuper = (*Volume)(nil)

func New() *Volume {
	return &Volume{
		s:        &mem.Mem{},
		mkv:      make(map[string]*blobs.Manifest),
		files:    make(map[string]*File),
		children: make(map[string]*fs.Inode),
	}
}
