package sod

import (
	"bytes"
	"compress/gzip"
	"compress/lzw"
	"compress/zlib"
	"encoding/json"
	"io"
	"io/ioutil"
	"testing"
)

var (
	data [][]byte
)

func init() {
	for o := range genTestStructs(10000) {
		if b, err := json.Marshal(o); err != nil {
			panic(err)
		} else {
			data = append(data, b)
		}
	}
}

func compress(b *testing.B, w io.Writer) (written int) {

	// compress
	for _, buf := range data {
		if n, err := w.Write(buf); err != nil {
			panic(err)
		} else {
			written += n
		}
	}

	return
}

func decompress(b *testing.B, r io.Reader) (read int) {

	// decompress
	if all, err := ioutil.ReadAll(r); err != nil {
		b.Error(err)
	} else {
		read += len(all)
	}

	return
}

// c -> compressed size u -> uncompressed size
func percCompressRatio(c, u int) float64 {
	return (1 - float64(c)/float64(u)) * 100
}

func BenchmarkMemcpy(b *testing.B) {
	buf := new(bytes.Buffer)
	written := compress(b, buf)

	b.Logf("Compression ratio=%.1f%%", percCompressRatio(buf.Len(), written))

	read := decompress(b, buf)
	if written != read {
		b.Error("data read different from written")
	}
}

func BenchmarkGzip(b *testing.B) {
	buf := new(bytes.Buffer)
	w := gzip.NewWriter(buf)
	written := compress(b, w)
	w.Close()

	b.Logf("Compression ratio=%.1f%%", percCompressRatio(buf.Len(), written))

	r, _ := gzip.NewReader(buf)
	read := decompress(b, r)
	if written != read {
		b.Error("data read different from written")
	}
}

func BenchmarkGzipBestSpeed(b *testing.B) {
	buf := new(bytes.Buffer)
	w, _ := gzip.NewWriterLevel(buf, gzip.BestSpeed)
	written := compress(b, w)
	w.Close()

	b.Logf("Compression ratio=%.1f%%", percCompressRatio(buf.Len(), written))

	r, _ := gzip.NewReader(buf)
	read := decompress(b, r)
	if written != read {
		b.Error("data read different from written")
	}
}

func BenchmarkLzw(b *testing.B) {
	buf := new(bytes.Buffer)
	w := lzw.NewWriter(buf, lzw.MSB, 8)
	written := compress(b, w)
	w.Close()

	b.Logf("Compression ratio=%.1f%%", percCompressRatio(buf.Len(), written))

	r := lzw.NewReader(buf, lzw.MSB, 8)
	read := decompress(b, r)
	if written != read {
		b.Error("data read different from written")
	}
	r.Close()
}

func BenchmarkZlib(b *testing.B) {
	buf := new(bytes.Buffer)
	w := zlib.NewWriter(buf)
	written := compress(b, w)
	w.Close()

	b.Logf("Compression ratio=%.1f%%", percCompressRatio(buf.Len(), written))

	r, _ := zlib.NewReader(buf)
	read := decompress(b, r)
	if written != read {
		b.Error("data read different from written")
	}
	r.Close()
}

/*func BenchmarkLz4(b *testing.B) {
	buf := new(bytes.Buffer)
	w := lz4.NewWriter(buf)
	written := compress(b, w)
	w.Close()

	b.Logf("Compression ratio=%.1f%%", percCompressRatio(buf.Len(), written))

	r := lz4.NewReader(buf)
	read := decompress(b, r)
	if written != read {
		b.Error("data read different from written")
	}
}*/
