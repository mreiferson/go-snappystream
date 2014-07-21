// snappystream wraps snappy-go and supplies a Reader and Writer
// for the snappy framed stream format:
//     https://snappy.googlecode.com/svn/trunk/framing_format.txt
package snappystream

import (
	"hash/crc32"
)

// Ext is the file extension for files whose content is a snappy framed stream.
const Ext = ".sz"

// MediaType is the MIME type used to represent snappy framed content.
const MediaType = "application/x-snappy-framed"

// ContentEncoding is the appropriate HTTP Content-Encoding header value for
// requests containing a snappy framed entity body.
const ContentEncoding = "x-snappy-framed"

// MaxBlockSize is the maximum number of decoded bytes allowed to be
// represented in a snappy framed block (sections 4.2 and 4.3).
const MaxBlockSize = 65536
const VerifyChecksum = true
const SkipVerifyChecksum = false

var crcTable *crc32.Table

func init() {
	crcTable = crc32.MakeTable(crc32.Castagnoli)
}
