package berry

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	SpecialVal      string = "SPECVAL"
	DataDir         string = "./data"
	MaxDataFileSize int64  = int64(200 * (1 << 20))
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
	activeDF, _ := NewDataFile(DataDir, 1)
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

	return b.set(b.active, key, []byte(val))
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

func (b *Berry) set(df *DataFile, key string, val []byte) error {
	e := NewEntry(key, val)
	data := e.Encode()

	offset, err := df.Write(data)
	if err != nil {
		return err
	}

	b.keydir[key] = Meta{
		FileID:      int32(df.ID()),
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

func (b *Berry) Merge(d time.Duration) {
	ticker := time.NewTicker(d).C

	for range ticker {
		b.Lock()
		b.merge()
		b.makeHintFile()
		b.Unlock()
	}
}

func (b *Berry) merge() error {
	// make a temp datafile
	tmpDir, err := os.MkdirTemp("", "merge")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	mdf, err := NewDataFile(tmpDir, 0)
	if err != nil {
		return err
	}

	// rewrite k-v into temp datafile
	for k := range b.keydir {
		v, _ := b.get(b.keydir[k])
		b.set(mdf, k, []byte(v))
	}

	// close active
	b.active.Close()

	// close all olders
	for _, df := range b.olders {
		df.Close()
	}

	b.olders = make(map[int32]*DataFile)

	// remove all datafile
	filepath.Walk(DataDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) == ".db" {
			err := os.Remove(path)
			if err != nil {
				return err
			}
		}
		return nil
	})

	// replace active datafile
	os.Rename(filepath.Join(tmpDir, fmt.Sprintf(DataFileNameFormat, 0)),
		filepath.Join(DataDir, fmt.Sprintf(DataFileNameFormat, 0)))

	b.active = mdf

	return nil
}

func (b *Berry) makeHintFile() error {
	path := filepath.Join(DataDir, "berry.hint")
	err := b.keydir.Encode(path)
	if err != nil {
		return err
	}

	return nil
}

func (b *Berry) CheckActiveFileSize(d time.Duration) {
	ticker := time.NewTicker(d).C

	for range ticker {
		b.checkActiveFileSize()
	}
}

func (b *Berry) checkActiveFileSize() error {
	b.Lock()
	defer b.Unlock()

	stat, err := b.active.fd.Stat()
	if err != nil {
		return err
	}

	size := stat.Size()
	if size < MaxDataFileSize {
		return nil
	}

	id := b.active.ID()
	b.olders[id] = b.active

	df, err := NewDataFile(DataDir, id+1)
	if err != nil {
		return err
	}

	b.active = df

	return nil
}
