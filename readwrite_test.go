package snappystream

import (
	"bytes"
	"testing"
)

func TestReaderWriter(t *testing.T) {
	var buf bytes.Buffer

	in := []byte("test")

	w := NewWriter(&buf)
	r := NewReader(&buf)
	r.VerifyChecksum = true

	n, err := w.Write(in)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if n != 4 {
		t.Fatalf("wrote wrong amount %d != 4", n)
	}

	out := make([]byte, 4)
	n, err = r.Read(out)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if n != 4 {
		t.Fatalf("read wrong amount %d != 4", n)
	}

	if !bytes.Equal(out, in) {
		t.Fatalf("bytes not equal %v != %v", out, in)
	}
}
