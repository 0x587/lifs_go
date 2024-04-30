package fuse

import (
	fs2 "github.com/hanwen/go-fuse/v2/fs"
	fuse2 "github.com/hanwen/go-fuse/v2/fuse"
	"lifs_go/cas/blobs"
	"lifs_go/cas/store"
	"lifs_go/fs"
)

type Impl struct {
	volume *Volume
}

func (i *Impl) Mount(dir string) (func(), error) {
	opts := fs2.Options{MountOptions: fuse2.MountOptions{Debug: false}}
	c := make(chan *fuse2.Server, 1)
	e := make(chan error, 1)
	go func() {
		server, err := fs2.Mount(dir, i.volume, &opts)
		if err != nil {
			e <- err
			return
		}
		c <- server
		server.Wait()
	}()
	select {
	case err := <-e:
		return nil, err
	case server := <-c:
		return func() {
			_ = server.Unmount()
		}, nil
	}
}

func open(store store.IF) *Volume {
	return &Volume{
		s:        store,
		mkv:      make(map[string]*blobs.Manifest),
		children: make(map[string]*fs2.Inode),
	}
}

func New(store store.IF) fs.IF {
	return &Impl{
		volume: open(store),
	}
}
