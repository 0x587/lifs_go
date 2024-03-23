package blobs

import "fmt"

// SmallChunkSizeError is the error returned from Open if the
// configuration has a ChunkSize less than MinChunkSize
type SmallChunkSizeError struct {
	Given uint32
}

var _ error = SmallChunkSizeError{}

func (s SmallChunkSizeError) Error() string {
	return fmt.Sprintf("[ErrBlob] ChunkSize is too small: %d < %d", s.Given, MinChunkSize)
}

// SmallFanoutError is the error returned from Open if the
// configuration has a Fanout less than 2.
type SmallFanoutError struct {
	Given uint32
}

var _ error = SmallFanoutError{}

func (s SmallFanoutError) Error() string {
	return fmt.Sprintf("[ErrBlob] Fanout is too small: %d", s.Given)
}
