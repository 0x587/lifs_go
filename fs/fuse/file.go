package fuse

import (
	"context"
	"fmt"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"io"
	"lifs_go/cas/blobs"
	"lifs_go/cas/store"
	"syscall"
)

type File struct {
	fs.Inode
	s    store.IF
	blob *blobs.Blob
}

func (f *File) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return f, 0, syscall.F_OK
}

var _ fs.NodeOpener = (*File)(nil)

func (f *File) Getattr(ctx context.Context, f_ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	fmt.Println("File.Getattr", f.blob.Size())
	out.Mode = fuse.S_IFREG
	out.Size = f.blob.Size()
	return syscall.F_OK
}

var _ fs.NodeGetattrer = (*File)(nil)

func (f *File) Setattr(ctx context.Context, f_ fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return syscall.F_OK
}

var _ fs.NodeSetattrer = (*File)(nil)

func (f *File) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	n, err := f.blob.IO(ctx).WriteAt(data, off)
	if err != nil {
		return 0, syscall.EBADMSG
	}
	return uint32(n), syscall.F_OK
}

var _ fs.FileWriter = (*File)(nil)

func (f *File) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	_, err := f.blob.IO(ctx).ReadAt(dest, off)
	if err != nil && err != io.EOF {
		return nil, syscall.EBADMSG
	}
	return fuse.ReadResultData(dest), syscall.F_OK
}

var _ fs.FileReader = (*File)(nil)

func newFile(s store.IF, m *blobs.Manifest) *File {
	b, _ := blobs.Open(s, m)
	return &File{
		s:    s,
		blob: b,
	}
}
