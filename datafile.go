package berry

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	activeDataFile = "berry_%d.db"
)

type DataFile struct {
	id     int32
	offset int32
	fd     *os.File
}

func NewDataFile(dir string, id int32) (*DataFile, error) {
	path := filepath.Join(dir, fmt.Sprintf(activeDataFile, id))
	fd, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open file %s error: %s", path, err.Error())
	}

	stat, err := fd.Stat()
	if err != nil {
		return nil, fmt.Errorf("get file %s stat error: %s", path, err.Error())
	}

	d := &DataFile{
		id:     id,
		fd:     fd,
		offset: int32(stat.Size()),
	}
	return d, nil
}

func (df *DataFile) ID() int32 {
	return df.id
}

func (df *DataFile) Write(data []byte) (int32, error) {
	_, err := df.fd.Write(data)
	if err != nil {
		return -1, err
	}

	offset := df.offset
	df.offset += int32(len(data))

	return offset, nil
}
