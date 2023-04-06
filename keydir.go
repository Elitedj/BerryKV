package berry

import (
	"encoding/gob"
	"os"
)

type KeyDir map[string]Meta

type Meta struct {
	FileID      int32
	EntrySize   int32
	EntryOffset int32
	Timestamp   int32
}

func (k *KeyDir) Encode(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(k)
	if err != nil {
		return err
	}

	return nil
}

func (k *KeyDir) Decode(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(k)
	if err != nil {
		return err
	}

	return nil
}
