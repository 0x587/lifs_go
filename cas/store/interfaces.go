package store

import (
	"context"
	"lifs_go/cas"
	"lifs_go/cas/chunks"
)

type Store interface {
	Get(ctx context.Context, key cas.Key, type_ string, level uint8) (*chunks.Chunk, error)
	Add(ctx context.Context, chunk *chunks.Chunk) (key cas.Key, err error)
}

type Handler func(ctx context.Context, key cas.Key, typ string, level uint8) ([]byte, error)

func HandleGet(ctx context.Context, fn Handler, key cas.Key, typ string, level uint8) (*chunks.Chunk, error) {
	if key.IsSpecial() {
		if key == cas.Empty {
			return chunks.MakeChunk(typ, level, nil), nil
		}
		return nil, cas.NotFoundError{
			Type:  typ,
			Level: level,
			Key:   key,
		}
	}

	data, err := fn(ctx, key, typ, level)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, cas.NotFoundError{
			Type:  typ,
			Level: level,
			Key:   key,
		}
	}
	c := chunks.MakeChunk(typ, level, data)
	return c, nil
}
