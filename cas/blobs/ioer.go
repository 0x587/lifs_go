package blobs

import (
	"context"
	"errors"
	"io"
)

// IO provides cancellable I/O operations on blobs.
type IO struct {
	ctx  context.Context
	blob *Blob
}

func (blob *Blob) IO(ctx context.Context) *IO {
	return &IO{ctx: ctx, blob: blob}
}

var _ io.ReaderAt = (*IO)(nil)

var _ io.WriterAt = (*IO)(nil)

// ReadAt reads data from the given offset. See io.ReaderAt.
func (bio *IO) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("negative offset is not possible")
	}
	{
		off := uint64(off)
		for {
			if off >= bio.blob.m.Size {
				return n, io.EOF
			}

			// avoid reading past EOF
			if uint64(len(p)) > bio.blob.m.Size-off {
				p = p[:int(bio.blob.m.Size-off)]
			}

			if len(p) == 0 {
				break
			}

			chunk, err := bio.blob.lookup(bio.ctx, off)
			if err != nil {
				return n, err
			}

			loff := uint32(off % uint64(bio.blob.m.ChunkSize))
			var copied int
			// TODO ugly int conversion
			if int(loff) <= len(chunk.Buf) {
				copied = copy(p, chunk.Buf[loff:])
			}
			for len(p) > copied && loff+uint32(copied) < bio.blob.m.ChunkSize {
				// handle case where chunk has been zero trimmed
				p[copied] = '\x00'
				copied++
			}
			n += copied
			p = p[copied:]
			off += uint64(copied)
		}

	}
	return n, err
}

// WriteAt writes data to the given offset. See io.WriterAt.
func (bio *IO) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("negative offset is not possible")
	}
	{
		off := uint64(off)
		for len(p) > 0 {

			chunk, err := bio.blob.lookupForWrite(bio.ctx, off)
			if err != nil {
				return n, err
			}

			leftOffset := uint32(off % uint64(bio.blob.m.ChunkSize))
			copied := copy(chunk.Buf[leftOffset:], p)
			n += copied
			p = p[copied:]
			off += uint64(copied)

			// off points now at the *next* byte that would be
			// written, so the "byte offset 0 is size 1" logic works
			// out here without -1's
			if off > bio.blob.m.Size {
				bio.blob.m.Size = off
			}
		}
	}
	return n, nil
}
