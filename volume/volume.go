package volume

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"lifs_go/kv"
	"path/filepath"
	"strings"
)

//var (
//	MetadataBucket = []byte("Metadata")
//	StorageBucket  = []byte("Storage")
//	FileDB         = ".lifs.db"
//)

var (
	ErrAlreadyInitialized = errors.New("this Volume is already initialized")
)

type Metadata struct {
	Path string
}

type Volume struct {
	RootPath string
	kv       kv.IF
	init     bool
}

func (v *Volume) Init() error {
	if v.init {
		return ErrAlreadyInitialized
	}
	return nil
}

func (v *Volume) Scan() error {
	rootComponents := strings.Split(filepath.ToSlash(v.RootPath), "/")
	visit := func(path string, info fs.FileInfo, err error) error {
		if path == v.RootPath {
			return nil
		}
		if err != nil {
			return err
		}
		pathComponents := strings.Split(filepath.ToSlash(path), "/")
		i := 0
		for i < len(rootComponents) {
			if pathComponents[i] != rootComponents[i] {
				break
			}
			i++
		}
		path = "/" + strings.Join(pathComponents[i:], "/")
		if info.IsDir() {
		} else {
			data, _ := json.Marshal(Metadata{Path: path})
			//TODO: ctx
			err := v.kv.Put(context.Background(), []byte(path), data)
			if err != nil {
				return err
			}
		}
		return nil
	}
	if err := filepath.Walk(v.RootPath, visit); err != nil {
		return err
	}
	return nil
}

func NewVolume(root string, kv kv.IF) (*Volume, error) {
	return &Volume{root, kv, false}, nil
}
