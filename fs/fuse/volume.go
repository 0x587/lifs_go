package fuse

import (
	"context"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"lifs_go/cas/blobs"
	"lifs_go/cas/store"
	"syscall"
)

type Volume struct {
	fs.Inode
	s        store.IF
	mkv      map[string]*blobs.Manifest
	children map[string]*fs.Inode
}

func (v *Volume) getChild(ctx context.Context, name string, f func() fs.InodeEmbedder) *fs.Inode {
	node, ok := v.children[name]
	if ok {
		return node
	}
	if f == nil {
		panic("try to get a not exist child but not provide make function.")
	}
	ie := f()
	var attr uint32
	switch ie.(type) {
	case *Volume:
		attr = fuse.S_IFDIR
	case *File:
		attr = fuse.S_IFREG
	}
	node = v.NewInode(ctx, f(), fs.StableAttr{Mode: attr})
	v.children[name] = node
	return node
}

func (v *Volume) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = fuse.S_IFDIR
	return syscall.F_OK
}

var _ fs.NodeGetattrer = (*Volume)(nil)

func (v *Volume) Create(ctx context.Context, name string, flags uint32, mode uint32, _ *fuse.EntryOut) (
	node *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	inode := v.getChild(ctx, name, func() fs.InodeEmbedder {
		m := blobs.EmptyManifest("file")
		v.mkv[name] = m
		return newFile(v.s, m)
	})
	return inode, inode.Operations(), flags, syscall.F_OK
}

var _ fs.NodeCreater = (*Volume)(nil)

func (v *Volume) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (
	*fs.Inode, syscall.Errno) {
	return v.getChild(ctx, name, func() fs.InodeEmbedder {
		return open(v.s)
	}), syscall.F_OK
}

var _ fs.NodeMkdirer = (*Volume)(nil)

func (v *Volume) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries := make([]fuse.DirEntry, 0)
	for name, _ := range v.children {
		entries = append(entries, fuse.DirEntry{Name: name})
	}
	res := fs.NewListDirStream(entries)
	return res, syscall.F_OK
}

var _ fs.NodeReaddirer = (*Volume)(nil)

func (v *Volume) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (
	*fs.Inode, syscall.Errno) {
	// for file
	_, ok := v.children[name]
	if ok {
		return v.getChild(ctx, name, nil), syscall.F_OK
	}
	return nil, syscall.ENOENT
}

var _ fs.NodeLookuper = (*Volume)(nil)
