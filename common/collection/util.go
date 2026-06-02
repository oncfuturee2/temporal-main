package collection

import (
	"encoding/binary"
	"encoding/hex"
)

// UUIDHashCode is a hash function for hashing string uuid
// if the uuid is malformed, then the hash function always
// returns 0 as the hash value
func UUIDHashCode(key string) uint32 {
	if len(key) != UUIDStringLength {
		return 0
	}
	b, err := hex.DecodeString(key[:8])
	if err != nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}
