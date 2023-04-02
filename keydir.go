package berry

type KeyDir map[string]Meta

type Meta struct {
	FileID      int32
	EntrySize   int32
	EntryOffset int32
	Timestamp   int32
}
