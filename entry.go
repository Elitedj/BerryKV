package berry

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

const (
	headerSize = 16
)

type Entry struct {
	CheckSum  uint32
	Timestamp uint32
	KeySize   uint32
	ValSize   uint32
	Key       string
	Value     []byte
}

func NewEntry(key string, value []byte) *Entry {
	e := &Entry{
		CheckSum:  crc32.ChecksumIEEE(value),
		Timestamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(key)),
		ValSize:   uint32(len(value)),
		Key:       key,
		Value:     value,
	}

	return e
}

func (e *Entry) Size() int32 {
	return int32(headerSize + e.KeySize + e.ValSize)
}

func (e *Entry) Encode() []byte {
	size := e.Size()
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf[0:4], e.CheckSum)
	binary.LittleEndian.PutUint32(buf[4:8], e.Timestamp)
	binary.LittleEndian.PutUint32(buf[8:12], e.KeySize)
	binary.LittleEndian.PutUint32(buf[12:16], e.ValSize)
	copy(buf[headerSize:headerSize+e.KeySize], []byte(e.Key))
	copy(buf[headerSize+e.KeySize:], e.Value)
	return buf
}
