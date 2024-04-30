package fuse

import (
	gofs "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"lifs_go/access"
	"lifs_go/cas/blobs"
	"lifs_go/cas/store"
)

type Impl struct {
	volume *Volume
}

func (i *Impl) Mount(dir string) (func(), error) {
	opts := gofs.Options{MountOptions: gofuse.MountOptions{Debug: false}}
	c := make(chan *gofuse.Server, 1)
	e := make(chan error, 1)
	go func() {
		server, err := gofs.Mount(dir, i.volume, &opts)
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
		children: make(map[string]*gofs.Inode),
	}
}

func New(store store.IF) access.IF {
	return &Impl{
		volume: open(store),
	}
}
