package crypto

import (
	"crypto/sha256"
	"fmt"
)

func GetHash(in string) string {
	h := sha256.New()

	h.Write([]byte(in))
	bs := h.Sum(nil)

	return fmt.Sprintf("%x", bs)
}
