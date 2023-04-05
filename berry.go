package berry

import (
	"errors"
	"sync"
	"time"
)

const (
	SpecialVal string = "SPECVAL"
)

var (
	ErrKeyNotFound      = errors.New("error: key not found")
	ErrDataFileNotFound = errors.New("error: data file not found")
)

type Berry struct {
	sync.Mutex
	active *DataFile
	olders map[int32]*DataFile
	keydir KeyDir
}

func New() (*Berry, error) {
	dir := "./data"
	activeDF, _ := NewDataFile(dir, 1)
	b := &Berry{
		active: activeDF,
		olders: make(map[int32]*DataFile),
		keydir: make(KeyDir),
	}
	return b, nil
}

func (b *Berry) Set(key, val string) error {
	b.Lock()
	defer b.Unlock()

	return b.set(key, []byte(val))
}

func (b *Berry) Get(key string) (string, error) {
	b.Lock()
	defer b.Unlock()

	meta, ok := b.keydir[key]
	if !ok {
		return "", ErrKeyNotFound
	}

	return b.get(meta)
}

func (b *Berry) Del(key string) error {
	b.Lock()
	defer b.Unlock()

	_, ok := b.keydir[key]
	if !ok {
		return nil
	}

	return b.del(key)
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

func (b *Berry) get(m Meta) (string, error) {
	fid := m.FileID
	var df *DataFile

	if fid == b.active.ID() {
		df = b.active
	} else {
		_, ok := b.olders[fid]
		if !ok {
			df = b.olders[fid]
		}
	}

	if df == nil {
		return "", ErrDataFileNotFound
	}

	return df.Read(m.EntryOffset, m.EntrySize)
}

func (b *Berry) del(key string) error {
	e := NewEntry(key, []byte(SpecialVal))
	data := e.Encode()

	_, err := b.active.Write(data)
	if err != nil {
		return err
	}

	delete(b.keydir, key)

	return nil
}
