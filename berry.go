package berry

import (
	"sync"
	"time"
)

type Berry struct {
	sync.Mutex
	active *DataFile
	olders map[int]*DataFile
	keydir KeyDir
}

func New() (*Berry, error) {
	dir := "./data"
	activeDF, _ := NewDataFile(dir, 1)
	b := &Berry{
		active: activeDF,
		olders: make(map[int]*DataFile),
		keydir: make(KeyDir),
	}
	return b, nil
}

func (b *Berry) Set(key, val string) error {
	b.Lock()
	defer b.Unlock()

	return b.set(key, []byte(val))
}

func (b *Berry) Get(key string) error {
	return nil
}

func (b *Berry) set(key string, val []byte) error {
	e := NewEntry(key, val)
	data := e.Encode()

	offset, err := b.active.Write(data)
	if err != nil {
		return err
	}

	b.keydir[key] = Meta{
		FileID:      int32(b.active.ID()),
		EntrySize:   int32(len(data)),
		EntryOffset: offset,
		Timestamp:   int32(time.Now().Unix()),
	}
	return nil
}
