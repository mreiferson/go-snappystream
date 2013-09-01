// snappystream wraps snappy-go and supplies a Reader and Writer
// for the snappy framed stream format:
//     https://snappy.googlecode.com/svn/trunk/framing_format.txt
package snappystream

const MaxBlockSize = 65536
