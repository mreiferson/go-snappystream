package snappystream

import (
	"bytes"
	"crypto/rand"
	"io"
	"io/ioutil"
	"testing"
)

const TestFileSize = 10 << 20 // 10MB

func testWriteThenRead(t *testing.T, name string, bs []byte) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	n, err := w.Write(bs)
	if err != nil {
		t.Errorf("write %v: %v", name, err)
		return
	}
	if n != len(bs) {
		t.Errorf("write %v: wrote %d bytes (!= %d)", name, n, len(bs))
		return
	}

	enclen := buf.Len()

	r := NewReader(&buf, true)
	gotbs, err := ioutil.ReadAll(r)
	if err != nil {
		t.Errorf("read %v: %v", name, err)
		return
	}
	n = len(gotbs)
	if n != len(bs) {
		t.Errorf("read %v: read %d bytes (!= %d)", name, n, len(bs))
		return
	}

	if !bytes.Equal(gotbs, bs) {
		t.Errorf("%v: unequal decompressed content", name)
		return
	}

	c := float64(len(bs)) / float64(enclen)
	t.Logf("%v compression ratio %.03g", name, c)
}

func TestWriterReader(t *testing.T) {
	testWriteThenRead(t, "simple", []byte("test"))
	testWriteThenRead(t, "manpage", testDataMan)
	testWriteThenRead(t, "json", testDataJSON)

	p := make([]byte, TestFileSize)
	testWriteThenRead(t, "constant", p)

	_, err := rand.Read(p)
	if err != nil {
		t.Fatal(err)
	}
	testWriteThenRead(t, "random", p)

}

func TestWriterChunk(t *testing.T) {
	var buf bytes.Buffer

	in := make([]byte, 128000)

	w := NewWriter(&buf)
	r := NewReader(&buf, VerifyChecksum)

	n, err := w.Write(in)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if n != len(in) {
		t.Fatalf("wrote wrong amount %d != %d", n, len(in))
	}

	out := make([]byte, len(in))
	n, err = io.ReadFull(r, out)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if n != len(in) {
		t.Fatalf("read wrong amount %d != %d", n, len(in))
	}

	if !bytes.Equal(out, in) {
		t.Fatalf("bytes not equal %v != %v", out, in)
	}
}

func BenchmarkWriterManpage(b *testing.B) {
	benchmarkWriterBytes(b, testDataMan)
}

func BenchmarkWriterJSON(b *testing.B) {
	benchmarkWriterBytes(b, testDataJSON)
}

// BenchmarkWriterRandom tests basically uncompressable data.
func BenchmarkWriterRandom(b *testing.B) {
	size := TestFileSize
	randp := make([]byte, size)
	_, err := rand.Read(randp)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkWriterBytes(b, randp)
}

// BenchmarkWriterConstant tests maximally compressible data
func BenchmarkWriterConstant(b *testing.B) {
	size := TestFileSize
	zerop := make([]byte, size)
	benchmarkWriterBytes(b, zerop)
}

func benchmarkWriterBytes(b *testing.B, p []byte) {
	b.SetBytes(int64(len(p)))
	w := NewWriter(ioutil.Discard)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		n, err := w.Write(p)
		if err != nil {
			b.Fatalf(err.Error())
		}
		if n != len(p) {
			b.Fatalf("wrote wrong amount %d != %d", n, len(p))
		}
	}
	b.StopTimer()
}

func BenchmarkReaderManpage(b *testing.B) {
	benchmarkReaderDiscard(b, testDataMan)
}

func BenchmarkReaderJSON(b *testing.B) {
	benchmarkReaderDiscard(b, testDataJSON)
}

// BenchmarkReaderRandom tests basically uncompressable data.
func BenchmarkReaderRandom(b *testing.B) {
	size := TestFileSize
	randp := make([]byte, size)
	_, err := rand.Read(randp)
	if err != nil {
		b.Fatal(err)
	}
	benchmarkReaderDiscard(b, randp)
}

// BenchmarkReaderConstant tests maximally compressible data
func BenchmarkReaderConstant(b *testing.B) {
	size := TestFileSize
	zerop := make([]byte, size)
	benchmarkReaderDiscard(b, zerop)
}

func benchmarkReaderDiscard(b *testing.B, p []byte) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	_, err := w.Write(p)
	if err != nil {
		b.Fatal("pre-test compression: %v", err)
	}
	encp := buf.Bytes()

	b.SetBytes(int64(len(encp)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := NewReader(bytes.NewReader(encp), true)
		n, err := io.Copy(ioutil.Discard, r)
		if err != nil {
			b.Fatalf(err.Error())
		}
		if n != int64(len(p)) {
			b.Fatalf("read wrong amount %d != %d", n, len(p))
		}
	}
	b.StopTimer()
}
