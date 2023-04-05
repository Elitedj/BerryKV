package berry_test

import (
	"testing"

	"github.com/elitedj/berry"
	"github.com/stretchr/testify/assert"
)

func TestSetAndGet(t *testing.T) {
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
