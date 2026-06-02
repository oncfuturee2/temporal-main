package collection

import (
	"encoding/binary"
	"encoding/hex"
)

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
