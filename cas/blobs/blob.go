package blobs

import (
	"context"
	"errors"
	"fmt"
	"lifs_go/cas"
	"lifs_go/cas/chunks"
	"lifs_go/cas/chunks/stash"
	"lifs_go/cas/store"
	"math"
)

var (
	ErrMissingType = errors.New("manifest is missing Type")
)

const (
	debugTruncate = true
	debugLookup   = true
)

const MinChunkSize = 4096

type Blob struct {
	stash *stash.Stash
	m     Manifest
	depth uint8
}

// Open returns a new Blob, using the given chunk store and manifest.
//
// It makes a copy of the manifest, so the caller is free to use it in
// any way after the call.
//
// A Blob need not exist; passing in a Manifest with an Empty Root
// gives a Blob with zero contents. However, all the fields must be
// set to valid values.
func Open(chunkStore store.IF, manifest *Manifest) (*Blob, error) {
	// make a copy so caller can't mutate it
	m := *manifest
	if m.Type == "" {
		return nil, ErrMissingType
	}
	if m.ChunkSize < MinChunkSize {
		return nil, SmallChunkSizeError{m.ChunkSize}
	}
	if m.Fanout < 2 {
		return nil, SmallFanoutError{m.Fanout}
	}
	blob := &Blob{
		stash: stash.New(chunkStore),
		m:     m,
		depth: 0,
	}
	blob.depth = blob.computeLevel(blob.m.Size)
	return blob, nil
}

// Truncate adjusts the size of the blob. If the new size is less than
// the old size, data past that point is lost. If the new size is
// greater than the old size, the new part is full of zeroes.
func (blob *Blob) Truncate(ctx context.Context, size uint64) error {
	switch {
	case size == 0:
		// special case shrink to nothing
		blob.m.Root = cas.Empty
		blob.m.Size = 0
		blob.stash.Clear()

	case size < blob.m.Size:
		// shrink

		// i really am starting to hate the idea of file offsets being
		// int64's, but can't fight all the windmills at once.
		if size > math.MaxInt64 {
			return errors.New("cannot discard past 63-bit file size")
		}

		// we know size>0 from above
		off := size - 1
		gidx := uint32(off / uint64(blob.m.ChunkSize))
		lidxs := localChunkIndexes(blob.m.Fanout, gidx)
		err := blob.shrink(ctx, uint8(len(lidxs)))
		if err != nil {
			return err
		}

		// we don't need to always cow here (if everything is
		// perfectly aligned / already zero), but it's a rare enough
		// case that let's not care for now
		//
		// TODO this makes a tight loop on Open and Save wasteful

		{
			// TODO clone all the way down to be able to trim leaf chunk,
			// abusing lookupForWrite for now

			// we know size > 0 from above
			_, err := blob.lookupForWrite(ctx, size-1)
			if err != nil {
				return err
			}
		}

		// now zero-fill on the right; guaranteed cow by the above kludge
		key := blob.m.Root
		if debugTruncate {
			if !key.IsPrivate() {
				panic(fmt.Errorf("Truncate root is not private: %v", key))
			}
		}
		for level := blob.depth; level > 0; level-- {
			chunk, err := blob.stash.Get(ctx, key, blob.m.Type, level)
			if err != nil {
				return err
			}
			err = blob.discardAfter(ctx, chunk, lidxs[level-1]+1, level)
			if err != nil {
				return err
			}
			keyoff := int64(lidxs[level-1]) * cas.KeySize
			keybuf := chunk.Buf[keyoff : keyoff+cas.KeySize]
			key = cas.NewKeyPrivate(keybuf)
			if debugTruncate {
				if !key.IsPrivate() {
					panic(fmt.Errorf("Truncate key at level %d not private: %v", level, key))
				}
			}
		}

		// and finally the leaf chunk
		chunk, err := blob.stash.Get(ctx, key, blob.m.Type, 0)
		if err != nil {
			return err
		}
		{
			// TODO is there anything to clear here; beware modulo wraparound

			// size is also the offset of the next byte
			loff := uint32(size % uint64(blob.m.ChunkSize))
			zeroSlice(chunk.Buf[loff:])
		}

		// TODO what's the right time to adjust size, wrt errors
		blob.m.Size = size

		// TODO unit tests that checks we don't leak chunks?

	case size > blob.m.Size:
		// grow
		off := size - 1
		gidx := uint32(off / uint64(blob.m.ChunkSize))
		lidxs := localChunkIndexes(blob.m.Fanout, gidx)
		err := blob.grow(ctx, uint8(len(lidxs)))
		if err != nil {
			return err
		}
		blob.m.Size = size
	}
	return nil
}

// Save persists the Blob into the Store and returns a new Manifest
// that can be passed to Open later.
func (blob *Blob) Save(ctx context.Context) (*Manifest, error) {
	// make sure the tree is optimal depth, as later we rely purely on
	// size to compute depth; this might happen because of errors on a
	// write/truncate path
	level := blob.computeLevel(blob.m.Size)
	switch {
	case blob.depth > level:
		err := blob.shrink(ctx, level)
		if err != nil {
			return nil, err
		}
	case blob.depth < level:
		err := blob.grow(ctx, level)
		if err != nil {
			return nil, err
		}
	}
	k, err := blob.saveChunk(ctx, blob.m.Root, blob.depth)
	if err != nil {
		return nil, err
	}
	blob.m.Root = k
	// make a copy to return
	m := blob.m
	return &m, nil
}

// Size returns the current byte size of the Blob.
func (blob *Blob) Size() uint64 {
	return blob.m.Size
}

func (blob *Blob) computeLevel(size uint64) uint8 {
	// convert size (count of bytes) to offset of last byte
	if size == 0 {
		return 0
	}

	off := size - 1
	idx := uint32(off / uint64(blob.m.ChunkSize))
	var level uint8
	for idx > 0 {
		idx /= blob.m.Fanout
		level++
	}
	return level
}

// Given a global chunk index, generate a list of local chunk indexes.
//
// The list needs to be generated bottom up, but we consume it top
// down, so generate it fully at the beginning and keep it as a slice.
func localChunkIndexes(fanout uint32, chunk uint32) []uint32 {
	// 6 is a good guess for max level of pointer chunks;
	// The reason is that (64)^6 = 2^36 is greater than 2**32
	// when the default fanout is set to 64
	//
	// 4MiB chunk size, uint32 chunk index -> 15PiB of data.
	// overflow just means an allocation.
	index := make([]uint32, 0, 6)

	for chunk > 0 {
		index = append(index, chunk%fanout)
		chunk /= fanout
	}
	return index
}

// safeSlice returns a slice of buf if possible, and where buf is not
// large enough to serve this slice, it returns a new slice of the
// right size. In case buf ends in the middle of the range, the
// available bytes are copied over to the new slice.
func safeSlice(buf []byte, low int, high int) []byte {
	if low >= high {
		return nil
	}
	if high <= len(buf) {
		return buf[low:high]
	}
	s := make([]byte, high-low)
	if low <= len(buf) {
		copy(s, buf[low:])
	}
	return s
}

func trim(b []byte) []byte {
	end := len(b)
	for end > 0 && b[end-1] == 0x00 {
		end--
	}
	return b[:end]
}

func (blob *Blob) chunkSizeForLevel(level uint8) uint32 {
	switch level {
	case 0:
		return blob.m.ChunkSize
	default:
		return blob.m.Fanout * cas.KeySize
	}
}

// lookup fetches the data chunk for given global byte offset.
//
// The returned Chunk remains zero trimmed.
//
// It may be a Private or a Normal chunk. For writable Chunks, call
// lookupForWrite instead.
func (blob *Blob) lookup(ctx context.Context, off uint64) (*chunks.Chunk, error) {
	globalIdx := uint32(off / uint64(blob.m.ChunkSize))
	localIds := localChunkIndexes(blob.m.Fanout, globalIdx)
	level := blob.depth

	// walk down from the root
	var ptrKey = blob.m.Root
	for ; level > 0; level-- {
		// follow pointer chunks
		var idx uint32
		if int(level)-1 < len(localIds) {
			idx = localIds[level-1]
		}

		chunk, err := blob.stash.Get(ctx, ptrKey, blob.m.Type, level)
		if err != nil {
			return nil, err
		}

		keyOffset := int64(idx) * cas.KeySize
		// zero trimming may have cut the key off, even in the middle
		// TODO ugly int conversion
		keyBuf := safeSlice(chunk.Buf, int(keyOffset), int(keyOffset+cas.KeySize))
		ptrKey = cas.NewKeyPrivate(keyBuf)
	}

	chunk, err := blob.stash.Get(ctx, ptrKey, blob.m.Type, 0)
	return chunk, err
}

// lookupForWrite fetches the data chunk for the given offset and
// ensures it is Private and reinflated, and thus writable.
func (blob *Blob) lookupForWrite(ctx context.Context, off uint64) (*chunks.Chunk, error) {
	globalIdx := uint32(off / uint64(blob.m.ChunkSize))
	localIds := localChunkIndexes(blob.m.Fanout, globalIdx)

	err := blob.grow(ctx, uint8(len(localIds)))
	if err != nil {
		return nil, err
	}

	level := blob.depth

	var parentChunk *chunks.Chunk
	{
		// clone root if necessary
		var k cas.Key
		var err error
		size := blob.chunkSizeForLevel(level)
		k, parentChunk, err = blob.stash.Clone(ctx, blob.m.Root, blob.m.Type, level, size)
		if err != nil {
			return nil, err
		}
		blob.m.Root = k
	}

	// walk down from the root
	var ptrKey = blob.m.Root
	for ; level > 0; level-- {
		// follow pointer chunks
		var idx uint32
		if int(level)-1 < len(localIds) {
			idx = localIds[level-1]
		}

		keyOffset := int64(idx) * cas.KeySize
		{
			k := cas.NewKeyPrivate(parentChunk.Buf[keyOffset : keyOffset+cas.KeySize])
			if k.IsReserved() {
				return nil, fmt.Errorf("invalid stored key: key @%d in %v is %v", keyOffset, ptrKey, parentChunk.Buf[keyOffset:keyOffset+cas.KeySize])
			}
			ptrKey = k
		}

		// clone it (nop if already cloned)
		size := blob.chunkSizeForLevel(level - 1)
		ptrKey, child, err := blob.stash.Clone(ctx, ptrKey, blob.m.Type, level-1, size)
		if err != nil {
			return nil, err
		}

		if debugLookup {
			if uint64(len(child.Buf)) != uint64(size) {
				panic(fmt.Errorf("lookupForWrite clone for level %d made weird size %d != %d, key %v", level-1, len(child.Buf), size, ptrKey))
			}
		}

		// update the key in parent
		n := copy(parentChunk.Buf[keyOffset:keyOffset+cas.KeySize], ptrKey.Bytes())
		if debugLookup {
			if n != cas.KeySize {
				panic(fmt.Errorf("lookupForWrite copied only %d of the key", n))
			}
		}
		parentChunk = child
	}

	if debugLookup {
		if parentChunk.Level != 0 {
			panic(fmt.Errorf("lookupForWrite got a non-leaf: %v", parentChunk.Level))
		}
		if uint64(len(parentChunk.Buf)) != uint64(blob.m.ChunkSize) {
			panic(fmt.Errorf("lookupForWrite got short leaf: %v", len(parentChunk.Buf)))
		}
	}

	return parentChunk, nil
}

func (blob *Blob) grow(ctx context.Context, level uint8) error {
	// grow hash tree upward if needed

	for blob.depth < level {
		key, chunk, err := blob.stash.Clone(ctx, cas.Empty, blob.m.Type, blob.depth+1, blob.m.Fanout*cas.KeySize)
		if err != nil {
			return err
		}

		copy(chunk.Buf, blob.m.Root.Bytes())
		blob.m.Root = key
		blob.depth++
	}
	return nil
}

// Decreases depth, always selecting only the leftmost tree,
// and dropping all Private chunks in the rest.
func (blob *Blob) shrink(ctx context.Context, level uint8) error {
	for blob.depth > level {
		chunk, err := blob.stash.Get(ctx, blob.m.Root, blob.m.Type, blob.depth)
		if err != nil {
			return err
		}

		if blob.m.Root.IsPrivate() {
			// blob.depth must be >0 if we're here, so it's always a
			// pointer chunk; iterate all non-first keys and drop
			// Private chunks
			err = blob.discardAfter(ctx, chunk, 1, blob.depth)
			if err != nil {
				return err
			}
		}

		// now all non-left top-level private nodes have been dropped
		keyBuf := safeSlice(chunk.Buf, 0, cas.KeySize)
		key := cas.NewKeyPrivate(keyBuf)
		blob.m.Root = key
		blob.depth--
	}
	return nil
}

// chunk must be a Private chunk
func (blob *Blob) discardAfter(ctx context.Context, chunk *chunks.Chunk, lidx uint32, level uint8) error {
	if level == 0 {
		return nil
	}
	for ; lidx < blob.m.Fanout; lidx++ {
		keyOffset := lidx * cas.KeySize
		keyBuf := chunk.Buf[keyOffset : keyOffset+cas.KeySize]
		key := cas.NewKeyPrivate(keyBuf)
		if key.IsPrivate() {
			// there can't be any Private chunks if they key wasn't Private
			chunk, err := blob.stash.Get(ctx, key, blob.m.Type, level-1)
			if err != nil {
				return err
			}
			err = blob.discardAfter(ctx, chunk, 0, level-1)
			if err != nil {
				return err
			}
			blob.stash.Drop(key)
		}
		copy(chunk.Buf[keyOffset:keyOffset+cas.KeySize], cas.Empty.Bytes())
	}
	return nil
}

func zeroSlice(p []byte) {
	for len(p) > 0 {
		p[0] = 0
		p = p[1:]
	}
}

func (blob *Blob) saveChunk(ctx context.Context, key cas.Key, level uint8) (cas.Key, error) {
	if !key.IsPrivate() {
		// already saved
		return key, nil
	}

	chunk, err := blob.stash.Get(ctx, key, blob.m.Type, level)
	if err != nil {
		return key, err
	}

	if level > 0 {
		for off := uint32(0); off+cas.KeySize <= uint32(len(chunk.Buf)); off += cas.KeySize {
			cur := cas.NewKeyPrivate(chunk.Buf[off : off+cas.KeySize])
			if cur.IsReserved() {
				return key, fmt.Errorf("invalid stored key: key @%d in %v is %v", off, key, chunk.Buf[off:off+cas.KeySize])
			}
			// recurses at most `level` deep
			saved, err := blob.saveChunk(ctx, cur, level-1)
			if err != nil {
				return key, err
			}
			copy(chunk.Buf[off:off+cas.KeySize], saved.Bytes())
		}
	}

	chunk.Buf = trim(chunk.Buf)
	return blob.stash.Save(ctx, key)
}
