package cas

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

const KeySize = 64

// Key
// [x] is mean an any 8 bytes
// a Key's struct is [x] [x] [x] [x] [x] [x] [x] [x]
// [0] [0] [0] [0] [0] [0] [0000 000 xx] [xx...] is a special key with 72-bits space (aka 9 bytes)
// [0] [0] [0] [0] [0] [0] [0000 000 00] [00...] is an Empty Key
// [0] [0] [0] [0] [0] [0] [0000 000 ff] [ff...] is an Invalid Key
// [0] [0] [0] [0] [0] [0] [0000 000 fe] [xx...] is a Private Key
// else if reserved for future use. Not valid input.
type Key struct {
	object [KeySize]byte
}

const SpecialPrefixSize = KeySize - 9

var specialPrefix = make([]byte, SpecialPrefixSize)
var Empty = Key{}
var Invalid = Key{newInvalidKey()}

func newInvalidKey() [KeySize]byte {
	var suffix = [...]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	var buf [KeySize]byte
	copy(buf[KeySize-len(suffix):], suffix[:])
	return buf
}

func (k *Key) specialKind() byte {
	return k.object[SpecialPrefixSize]
}

func (k *Key) IsSpecial() bool {
	return bytes.HasPrefix(k.object[:], specialPrefix)
}

func (k *Key) IsPrivate() bool {
	return k.IsSpecial() && k.specialKind() == 0xFE
}

func (k *Key) IsReserved() bool {
	return k.IsSpecial() && k.specialKind() != 0xFE && *k != Empty
}

func (k *Key) Private() (num uint64, ok bool) {
	if !k.IsPrivate() {
		return 0, false
	}

	num = binary.BigEndian.Uint64(k.object[SpecialPrefixSize+1:])
	return num, true
}

type BadKeySizeError struct {
	Key []byte
}

var _ error = (*BadKeySizeError)(nil)

func (e *BadKeySizeError) Error() string {
	return fmt.Sprintf("[ErrKey] Key is bad length %d: %x", len(e.Key), e.Key)
}

func (k *Key) String() string {
	return hex.EncodeToString(k.object[:])
}

func (k *Key) Bytes() []byte {
	buf := make([]byte, KeySize)
	copy(buf, k.object[:])
	return buf
}

var _ encoding.BinaryMarshaler = (*Key)(nil)
var _ encoding.BinaryUnmarshaler = (*Key)(nil)

func (k *Key) MarshalBinary() (data []byte, err error) {
	data = make([]byte, KeySize)
	copy(data, k.Bytes())
	return data, nil
}

func (k *Key) UnmarshalBinary(data []byte) error {
	if len(data) != KeySize {
		return &BadKeySizeError{Key: data}
	}
	*k = NewKey(data)
	return nil
}

func newKey(b []byte) Key {
	k := Key{}
	n := copy(k.object[:], b)
	if n != KeySize {
		panic(BadKeySizeError{Key: b})
	}
	return k
}

func NewKey(b []byte) Key {
	k := newKey(b)
	if k.IsSpecial() && k != Empty {
		return Invalid
	}
	return k
}

func NewKeyPrivate(b []byte) Key {
	k := newKey(b)
	if k.IsSpecial() && !k.IsPrivate() && k != Empty {
		return Invalid
	}
	return k
}

func NewKeyPrivateNum(num uint64) Key {
	k := Key{}
	copy(k.object[:], specialPrefix)
	k.object[SpecialPrefixSize] = 0xFE
	binary.BigEndian.PutUint64(k.object[SpecialPrefixSize+1:], num)
	return k
}
