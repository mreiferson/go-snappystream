package snappystream

import (
	"code.google.com/p/snappy-go/snappy"
	"hash/crc32"
	"io"
)

// includes block header
var streamID = []byte{0xff, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59}

type Writer struct {
	writer       io.Writer
	hdr          []byte
	dst          []byte
	sentStreamID bool
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{
		writer: w,

		hdr: make([]byte, 8),
		dst: make([]byte, 4096),
	}
}

func (w *Writer) Write(p []byte) (int, error) {
	var err error

	w.dst, err = snappy.Encode(w.dst, p)
	if err != nil {
		return 0, err
	}

	if !w.sentStreamID {
		_, err := w.writer.Write(streamID)
		if err != nil {
			return 0, err
		}
		w.sentStreamID = true
	}

	length := uint32(len(w.dst)) + 4 // +4 for checksum

	w.hdr[0] = 0x00 // compressed frame ID

	// 3 byte little endian length
	w.hdr[1] = byte(length)
	w.hdr[2] = byte(length >> 8)
	w.hdr[3] = byte(length >> 16)

	// 4 byte little endian CRC32 checksum
	checksum := maskChecksum(crc32.ChecksumIEEE(p))
	w.hdr[4] = byte(checksum)
	w.hdr[5] = byte(checksum >> 8)
	w.hdr[6] = byte(checksum >> 16)
	w.hdr[7] = byte(checksum >> 24)

	_, err = w.writer.Write(w.hdr)
	if err != nil {
		return 0, err
	}

	_, err = w.writer.Write(w.dst)
	if err != nil {
		return 0, err
	}

	return len(p), nil
}

func maskChecksum(c uint32) uint32 {
	return ((c >> 15) | (c << 17)) + 0xa282ead8
}
