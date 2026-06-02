package collection

import (
	"encoding/binary"
	"encoding/hex"
)

// UUIDHashCode is a hash function for hashing string uuid
// if the uuid is malformed, then the hash function always
// returns 0 as the hash value
func UUIDHashCode(input string) uint32 {
	if len(input) != UUIDStringLength {
		return 0
	}
	// Use the first 4 bytes of the uuid as the hash
	b, err := hex.DecodeString(input[:8])
	if err != nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}
