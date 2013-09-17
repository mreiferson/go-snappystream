// snappystream wraps snappy-go and supplies a Reader and Writer
// for the snappy framed stream format:
//     https://snappy.googlecode.com/svn/trunk/framing_format.txt
package snappystream

import (
	"hash/crc32"
)

const MaxBlockSize = 65536
const VerifyChecksum = true
const SkipVerifyChecksum = false

var crcTable *crc32.Table

func init() {
	crcTable = crc32.MakeTable(crc32.Castagnoli)
}
