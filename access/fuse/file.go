package fuse

import (
	"context"
	"fmt"
	gofs "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"io"
	"lifs_go/cas/blobs"
	"lifs_go/cas/store"
	"syscall"
)

type File struct {
	gofs.Inode
	s    store.IF
	blob *blobs.Blob
}

func (f *File) Open(ctx context.Context, flags uint32) (fh gofs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	return f, 0, syscall.F_OK
}

var _ gofs.NodeOpener = (*File)(nil)

func (f *File) Getattr(ctx context.Context, f_ gofs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	fmt.Println("File.Getattr", f.blob.Size())
	out.Mode = gofuse.S_IFREG
	out.Size = f.blob.Size()
	return syscall.F_OK
}

var _ gofs.NodeGetattrer = (*File)(nil)

func (f *File) Setattr(ctx context.Context, f_ gofs.FileHandle, in *gofuse.SetAttrIn, out *gofuse.AttrOut) syscall.Errno {
	return syscall.F_OK
}

var _ gofs.NodeSetattrer = (*File)(nil)

func (f *File) Write(ctx context.Context, data []byte, off int64) (written uint32, errno syscall.Errno) {
	n, err := f.blob.IO(ctx).WriteAt(data, off)
	if err != nil {
		return 0, syscall.EBADMSG
	}
	return uint32(n), syscall.F_OK
}

var _ gofs.FileWriter = (*File)(nil)

func (f *File) Read(ctx context.Context, dest []byte, off int64) (gofuse.ReadResult, syscall.Errno) {
	_, err := f.blob.IO(ctx).ReadAt(dest, off)
	if err != nil && err != io.EOF {
		return nil, syscall.EBADMSG
	}
	return gofuse.ReadResultData(dest), syscall.F_OK
}

var _ gofs.FileReader = (*File)(nil)

func newFile(s store.IF, m *blobs.Manifest) *File {
	b, _ := blobs.Open(s, m)
	return &File{
		s:    s,
		blob: b,
	}
}
