package snappystream

import (
	"bytes"
	"code.google.com/p/snappy-go/snappy"
	"encoding/binary"
	"errors"
	"io"
)

type Reader struct {
	io.Reader
	buf bytes.Buffer
	hdr []byte
	src []byte
	dst []byte
}

func NewReader(r io.Reader) *Reader {
	return &Reader{
		Reader: r,
		hdr:    make([]byte, 4),
		src:    make([]byte, 4096),
		dst:    make([]byte, 4096),
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
		_, err := io.ReadFull(r.Reader, r.hdr)
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
			r.dst, err = snappy.Decode(r.dst, buf)
			if err != nil {
				return err
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
	length := binary.LittleEndian.Uint32(r.hdr[1:4])
	if int(length) > len(r.src) {
		r.src = make([]byte, length)
	}
	buf := r.src[:length]
	_, err := io.ReadFull(r.Reader, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}
