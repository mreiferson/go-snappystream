package snappystream

import (
	"bytes"
	"code.google.com/p/snappy-go/snappy"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

type Reader struct {
	VerifyChecksum bool

	reader io.Reader

	buf bytes.Buffer
	hdr []byte
	src []byte
	dst []byte
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		reader: r,

		hdr: make([]byte, 4),
		src: make([]byte, 4096),
		dst: make([]byte, 4096),
	}
}

func (r *Reader) Read(b []byte) (int, error) {
	if r.buf.Len() < len(b) {
		err := r.nextFrame()
		if err != nil {
			return 0, err
		}
	}
	return r.buf.Read(b)
}

func (r *Reader) nextFrame() error {
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
		case 0x00:
			// compressed bytes
			// first 4 bytes are the little endian crc32 checksum
			checksum := unmaskChecksum(uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16 | uint32(buf[3])<<24)
			r.dst, err = snappy.Decode(r.dst, buf[4:])
			if err != nil {
				return err
			}
			if r.VerifyChecksum {
				actualChecksum := crc32.ChecksumIEEE(r.dst)
				if checksum != actualChecksum {
					return errors.New(fmt.Sprintf("invalid checksum %x != %x", checksum, actualChecksum))
				}
			}
			_, err = r.buf.Write(r.dst)
			return err
		case 0x01:
			// uncompressed bytes
			_, err = r.buf.Write(buf)
			return err
		case 0xff:
			// stream identifier
			if !bytes.Equal(buf, []byte{0x73, 0x4e, 0x61, 0x50, 0x70, 0x59}) {
				return errors.New("invalid stream ID")
			}
			// continue...
		}
	}
}

func (r *Reader) readBlock() ([]byte, error) {
	// 3 byte little endian length
	length := uint32(r.hdr[1]) | uint32(r.hdr[2])<<8 | uint32(r.hdr[3])<<16

	if length > MaxBlockSize {
		return nil, errors.New(fmt.Sprintf("block too large %d > %d", length, MaxBlockSize))
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
