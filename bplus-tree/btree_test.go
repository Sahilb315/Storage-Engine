package bplustree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	b := New(3)

	err := b.Insert([]byte("a"), "v0")
	assert.NoError(t, err)

	err = b.Insert([]byte("b"), "v1")
	assert.NoError(t, err)
}

func TestGet(t *testing.T) {
	b := New(3)

	// prepare
	_ = b.Insert([]byte("a"), "v0")
	_ = b.Insert([]byte("b"), "v1")

	// retrieve existing
	v, err := b.Get([]byte("b"))
	assert.NoError(t, err)
	assert.Equal(t, "v1", v)

	// missing key
	_, err = b.Get([]byte("z"))
	assert.Error(t, err)
}

func TestDelete(t *testing.T) {
	b := New(3)

	// prepare
	_ = b.Insert([]byte("a"), "v0")
	_ = b.Insert([]byte("b"), "v1")

	// delete one key
	err := b.Delete([]byte("a"))
	assert.NoError(t, err)

	// ensure deleted
	_, err = b.Get([]byte("a"))
	assert.Error(t, err)

	// ensure other key still exists
	v, err := b.Get([]byte("b"))
	assert.NoError(t, err)
	assert.Equal(t, "v1", v)
}
