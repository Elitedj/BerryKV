package berry_test

import (
	"testing"

	"github.com/elitedj/berry"
	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	db, err := berry.New()
	assert.NoError(t, err)
	err = db.Set("Hello", "World")
	assert.NoError(t, err)
}
