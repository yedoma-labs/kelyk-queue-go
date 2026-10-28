package kelyk

import (
	"fmt"
	"unicode/utf8"
)

const maxKeyLength = 256

// validateKey accepts keys with 1 to 256 characters (runes). Any other content is allowed.
func validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("%w: must not be empty", ErrInvalidKey)
	}
	if utf8.RuneCountInString(key) > maxKeyLength {
		return fmt.Errorf("%w: must have at most %d characters", ErrInvalidKey, maxKeyLength)
	}
	return nil
}
