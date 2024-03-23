package kvfile

import (
	"context"
	"encoding/hex"
	"lifs_go/kv"
	"os"
	"path/filepath"
)

type KvFile struct {
	path string
}

var _ kv.KV = (*KvFile)(nil)

func (k *KvFile) key2FileName(key []byte) string {
	return filepath.Join(k.path, "."+hex.EncodeToString(key)+".data")
}

func (k *KvFile) Get(ctx context.Context, key []byte) ([]byte, error) {
	file, err := os.ReadFile(k.key2FileName(key))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, kv.NotFoundError{Key: key}
		}
		return nil, err
	}
	return file, nil
}

func (k *KvFile) Put(ctx context.Context, key, value []byte) (err error) {
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

func Open(path string) (*KvFile, error) {
	return &KvFile{path: path}, nil
}
