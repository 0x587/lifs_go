package blobs

import "lifs_go/cas"

type Manifest struct {
	Type string
	Root cas.Key
	Size uint64
	// Must be >= MinChunkSize.
	ChunkSize uint32
	// Must be >= 2.
	Fanout uint32
}

// EmptyManifest returns an empty manifest of the given type with the
// default tuning parameters.
func EmptyManifest(type_ string) *Manifest {
	const kB = 1024
	const MB = 1024 * kB

	return &Manifest{
		Type:      type_,
		ChunkSize: 4 * MB,
		Fanout:    64,
	}
}
