package fuse

import (
	"context"
	gofs "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"lifs_go/cas/blobs"
	"lifs_go/cas/store"
	"syscall"
)

type Volume struct {
	gofs.Inode
	s        store.IF
	mkv      map[string]*blobs.Manifest
	children map[string]*gofs.Inode
}

func (v *Volume) getChild(ctx context.Context, name string, f func() gofs.InodeEmbedder) *gofs.Inode {
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
		attr = gofuse.S_IFDIR
	case *File:
		attr = gofuse.S_IFREG
	}
	node = v.NewInode(ctx, f(), gofs.StableAttr{Mode: attr})
	v.children[name] = node
	return node
}

func (v *Volume) Getattr(ctx context.Context, f gofs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	out.Mode = gofuse.S_IFDIR
	return syscall.F_OK
}

var _ gofs.NodeGetattrer = (*Volume)(nil)

func (v *Volume) Create(ctx context.Context, name string, flags uint32, mode uint32, _ *gofuse.EntryOut) (
	node *gofs.Inode, fh gofs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	inode := v.getChild(ctx, name, func() gofs.InodeEmbedder {
		m := blobs.EmptyManifest("file")
		v.mkv[name] = m
		return newFile(v.s, m)
	})
	return inode, inode.Operations(), flags, syscall.F_OK
}

var _ gofs.NodeCreater = (*Volume)(nil)

func (v *Volume) Mkdir(ctx context.Context, name string, mode uint32, out *gofuse.EntryOut) (
	*gofs.Inode, syscall.Errno) {
	return v.getChild(ctx, name, func() gofs.InodeEmbedder {
		return open(v.s)
	}), syscall.F_OK
}

var _ gofs.NodeMkdirer = (*Volume)(nil)

func (v *Volume) Readdir(ctx context.Context) (gofs.DirStream, syscall.Errno) {
	entries := make([]gofuse.DirEntry, 0)
	for name, _ := range v.children {
		entries = append(entries, gofuse.DirEntry{Name: name})
	}
	res := gofs.NewListDirStream(entries)
	return res, syscall.F_OK
}

var _ gofs.NodeReaddirer = (*Volume)(nil)

func (v *Volume) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (
	*gofs.Inode, syscall.Errno) {
	// for file
	_, ok := v.children[name]
	if ok {
		return v.getChild(ctx, name, nil), syscall.F_OK
	}
	return nil, syscall.ENOENT
}

var _ gofs.NodeLookuper = (*Volume)(nil)
