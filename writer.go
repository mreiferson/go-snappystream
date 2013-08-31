package snappystream

import (
	"bytes"
	"code.google.com/p/snappy-go/snappy"
	"encoding/binary"
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
	var buf bytes.Buffer
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

	err = binary.Write(&buf, binary.LittleEndian, uint32(len(w.dst)))
	if err != nil {
		return 0, err
	}
	copy(w.hdr[1:4], buf.Bytes()[:3])

	_, err = w.Write(w.hdr)
	if err != nil {
		return 0, err
	}
	return w.Writer.Write(w.dst)
}
