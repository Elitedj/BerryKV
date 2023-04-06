package berry_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/elitedj/berry"
	"github.com/stretchr/testify/assert"
)

func CleanDataFile() {
	files, _ := ioutil.ReadDir(berry.DataDir)
	for _, file := range files {
		os.RemoveAll(filepath.Join(berry.DataDir, file.Name()))
	}
}

func TestSetAndGet(t *testing.T) {
	CleanDataFile()
	db, err := berry.New()
	assert.NoError(t, err)

	err = db.Set("Hello", "World")
	assert.NoError(t, err)
	val, err := db.Get("Hello")
	assert.NoError(t, err)
	assert.Equal(t, "World", val)

	err = db.Set("Hello", "berry kv")
	assert.NoError(t, err)
	val, err = db.Get("Hello")
	assert.NoError(t, err)
	assert.Equal(t, "berry kv", val)

	err = db.Set("key", "value")
	assert.NoError(t, err)
	val, err = db.Get("key")
	assert.NoError(t, err)
	assert.Equal(t, "value", val)

	val, err = db.Get("NoThisKey")
	assert.Equal(t, berry.ErrKeyNotFound, err)
}

func TestDel(t *testing.T) {
	CleanDataFile()
	db, err := berry.New()
	assert.NoError(t, err)

	err = db.Set("Hello", "World")
	assert.NoError(t, err)
	val, err := db.Get("Hello")
	assert.NoError(t, err)
	assert.Equal(t, "World", val)

	err = db.Del("Hello")
	assert.NoError(t, err)
	val, err = db.Get("Hello")
	assert.Equal(t, berry.ErrKeyNotFound, err)
}

func TestMerge(t *testing.T) {
	CleanDataFile()
	db, err := berry.New()
	assert.NoError(t, err)

	err = db.Set("Hello", "World")
	assert.NoError(t, err)
	val, err := db.Get("Hello")
	assert.NoError(t, err)
	assert.Equal(t, "World", val)

	go db.Merge(5 * time.Second)
	time.Sleep(7 * time.Second)
	val, err = db.Get("Hello")
	assert.NoError(t, err)
	assert.Equal(t, "World", val)
}

func TestCheckActiveFileSize(t *testing.T) {
	CleanDataFile()
	db, err := berry.New()
	assert.NoError(t, err)

	go db.CheckActiveFileSize(time.Second)

	for i := 1; i <= 9000000; i++ {
		db.Set("Hello", "World")
	}

	val, err := db.Get("Hello")
	assert.NoError(t, err)
	assert.Equal(t, "World", val)
}
