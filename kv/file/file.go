package file

import (
	"context"
	"encoding/hex"
	"lifs_go/kv"
	"os"
	"path/filepath"
)

type Impl struct {
	path string
}

func (k *Impl) key2FileName(key []byte) string {
	return filepath.Join(k.path, "."+hex.EncodeToString(key)+".data")
}

func (k *Impl) Get(ctx context.Context, key []byte) ([]byte, error) {
	file, err := os.ReadFile(k.key2FileName(key))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, kv.NotFoundError{Key: key}
		}
		return nil, err
	}
	return file, nil
}

func (k *Impl) Put(ctx context.Context, key, value []byte) (err error) {
	temp, err := os.CreateTemp(k.path, "put-")
	if err != nil {
		return err
	}
	defer func() {
		_ = temp.Close()
		_ = os.Remove(temp.Name())
	}()

	_, err = temp.Write(value)
	if err != nil {
		return err
	}
	err = os.Link(temp.Name(), k.key2FileName(key))
	if err != nil && !os.IsExist(err) {
		return err
	}
	return nil
}

func New(path string) kv.IF {
	return &Impl{path: path}
}
