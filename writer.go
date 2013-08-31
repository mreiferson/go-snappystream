package snappystream

import (
	"code.google.com/p/snappy-go/snappy"
	"io"
)

// includes block header
var streamID = []byte{0xff, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59}

type Writer struct {
	io.Writer
	hdr          []byte
	dst          []byte
	sentStreamID bool
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{
		Writer: w,
		hdr:    make([]byte, 4),
		dst:    make([]byte, 4096),
	}
}

func (w *Writer) Write(p []byte) (int, error) {
	var err error

	w.dst, err = snappy.Encode(w.dst, p)
	if err != nil {
		return 0, err
	}

	if !w.sentStreamID {
		_, err := w.Writer.Write(streamID)
		if err != nil {
			return 0, err
		}
		w.sentStreamID = true
	}

	length := uint32(len(w.dst))
	w.hdr[0] = 0x00 // compressed frame ID
	w.hdr[1] = byte(length)
    w.hdr[2] = byte(length >> 8)
    w.hdr[3] = byte(length >> 16)
	_, err = w.Writer.Write(w.hdr)
	if err != nil {
		return 0, err
	}
	_, err = w.Writer.Write(w.dst)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
