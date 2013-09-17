package snappystream

import (
	"bytes"
	"code.google.com/p/snappy-go/snappy"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

type reader struct {
	reader io.Reader

	verifyChecksum bool

	buf bytes.Buffer
	hdr []byte
	src []byte
	dst []byte
}

// NewReader returns an io.Reader interface to the snappy framed stream format.
//
// It transparently handles reading the stream identifier (but does not proxy this
// to the caller), decompresses blocks, and (optionally) validates checksums.
//
// Internally, three buffers are maintained.  The first two are for reading
// off the wrapped io.Reader and for holding the decompressed block (both are grown
// automatically and re-used and will never exceed the largest block size, 65536). The
// last buffer contains the *unread* decompressed bytes (and can grow indefinitely).
//
// The second param determines whether or not the reader will verify block
// checksums and can be enabled/disabled with the constants VerifyChecksum and SkipVerifyChecksum
//
// For each Read, the returned length will be up to the lesser of len(b) or 65536
// decompressed bytes, regardless of the length of *compressed* bytes read
// from the wrapped io.Reader.
func NewReader(r io.Reader, verifyChecksum bool) io.Reader {
	return &reader{
		reader: r,

		verifyChecksum: verifyChecksum,

		hdr: make([]byte, 4),
		src: make([]byte, 4096),
		dst: make([]byte, 4096),
	}
}

func (r *reader) Read(b []byte) (int, error) {
	if r.buf.Len() < len(b) {
		err := r.nextFrame()
		if err != nil {
			return 0, err
		}
	}
	return r.buf.Read(b)
}

func (r *reader) nextFrame() error {
	for {
		_, err := io.ReadFull(r.reader, r.hdr)
		if err != nil {
			return err
		}

		buf, err := r.readBlock()
		if err != nil {
			return err
		}

		switch r.hdr[0] {
		case 0x00, 0x01:
			// compressed or uncompressed bytes

			// first 4 bytes are the little endian crc32 checksum
			checksum := unmaskChecksum(uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24)
			b := buf[4:]

			if r.hdr[0] == 0x00 {
				// compressed bytes
				r.dst, err = snappy.Decode(r.dst, b)
				if err != nil {
					return err
				}
				b = r.dst
			}

			if r.verifyChecksum {
				actualChecksum := crc32.Checksum(b, crcTable)
				if checksum != actualChecksum {
					return errors.New(fmt.Sprintf("invalid checksum %x != %x", checksum, actualChecksum))
				}
			}

			_, err = r.buf.Write(b)
			return err
		case 0xff:
			// stream identifier
			if !bytes.Equal(buf, []byte{0x73, 0x4e, 0x61, 0x50, 0x70, 0x59}) {
				return errors.New("invalid stream ID")
			}
			// continue...
		default:
			return errors.New("invalid frame identifier")
		}
	}
	panic("should never happen")
}

func (r *reader) readBlock() ([]byte, error) {
	// 3 byte little endian length
	length := uint32(r.hdr[1]) | uint32(r.hdr[2])<<8 | uint32(r.hdr[3])<<16

	// +4 for checksum
	if length > (MaxBlockSize + 4) {
		return nil, errors.New(fmt.Sprintf("block too large %d > %d", length, (MaxBlockSize + 4)))
	}

	if int(length) > len(r.src) {
		r.src = make([]byte, length)
	}

	buf := r.src[:length]
	_, err := io.ReadFull(r.reader, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

func unmaskChecksum(c uint32) uint32 {
	x := c - 0xa282ead8
	return ((x >> 17) | (x << 15))
}
