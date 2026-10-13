package mediaimage

import (
	"encoding/binary"
	"fmt"
)

// These scanners walk length-delimited containers without decoding frame pixels.
// Every iteration consumes input and every length is checked before slicing.
func inspectPNG(data []byte) error {
	pos := 8
	for pos < len(data) {
		if len(data)-pos < 12 {
			return ErrInvalidImage
		}
		n := uint64(binary.BigEndian.Uint32(data[pos : pos+4]))
		if n > uint64(len(data)-pos-12) {
			return ErrInvalidImage
		}
		kind := string(data[pos+4 : pos+8])
		switch kind {
		case "acTL", "fcTL", "fdAT":
			return fmt.Errorf("%w: animated PNG", ErrUnsupportedImage)
		}
		pos += 12 + int(n)
		if kind == "IEND" {
			if n != 0 || pos != len(data) {
				return ErrInvalidImage
			}
			return nil
		}
	}
	return ErrInvalidImage
}

func inspectWebP(data []byte) error {
	if uint64(binary.LittleEndian.Uint32(data[4:8]))+8 != uint64(len(data)) {
		return ErrInvalidImage
	}
	pos := 12
	for pos < len(data) {
		if len(data)-pos < 8 {
			return ErrInvalidImage
		}
		n := uint64(binary.LittleEndian.Uint32(data[pos+4 : pos+8]))
		padded := n + (n & 1)
		if padded > uint64(len(data)-pos-8) {
			return ErrInvalidImage
		}
		kind := string(data[pos : pos+4])
		if kind == "ANIM" || kind == "ANMF" || (kind == "VP8X" && n > 0 && data[pos+8]&2 != 0) {
			return fmt.Errorf("%w: animated WebP", ErrUnsupportedImage)
		}
		pos += 8 + int(padded)
	}
	return nil
}

func inspectGIF(data []byte) error {
	if len(data) < 13 {
		return ErrInvalidImage
	}
	pos := 13
	if data[10]&0x80 != 0 {
		pos += 3 * (1 << ((data[10] & 7) + 1))
	}
	if pos > len(data) {
		return ErrInvalidImage
	}
	frames := 0
	// Skip GIF sub-blocks (extensions and compressed pixels) without allocating.
	skip := func() bool {
		for pos < len(data) {
			n := int(data[pos])
			pos++
			if n == 0 {
				return true
			}
			if n > len(data)-pos {
				return false
			}
			pos += n
		}
		return false
	}
	for pos < len(data) {
		kind := data[pos]
		pos++
		switch kind {
		case 0x3b:
			if frames != 1 || pos != len(data) {
				return ErrInvalidImage
			}
			return nil
		case 0x21:
			if pos >= len(data) {
				return ErrInvalidImage
			}
			pos++ // extension label; payload is a sequence of sub-blocks
			if !skip() {
				return ErrInvalidImage
			}
		case 0x2c:
			frames++
			if frames > 1 {
				return fmt.Errorf("%w: animated GIF", ErrUnsupportedImage)
			}
			if len(data)-pos < 9 {
				return ErrInvalidImage
			}
			packed := data[pos+8]
			pos += 9
			if packed&0x80 != 0 {
				pos += 3 * (1 << ((packed & 7) + 1))
			}
			if pos >= len(data) {
				return ErrInvalidImage
			}
			pos++ // LZW minimum code size
			if !skip() {
				return ErrInvalidImage
			}
		default:
			return ErrInvalidImage
		}
	}
	return ErrInvalidImage
}
