package bplustree

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRange(t *testing.T) {
	b := New(3)

	for i := range 20 {
		b.InsertInt(i, []byte(fmt.Sprintf("Value for %d", i)))
	}

	ite := b.SeekFirst()
	assert.NotNil(t, ite)

	values := make([][]byte, 0)

	for ite.Valid() {
		values = append(values, ite.Value())

		ite.Next()
	}

	for i := range 20 {
		assert.Equal(t, []byte(fmt.Sprintf("Value for %d", i)), values[i])
	}
}

func TestSeek(t *testing.T) {
	b := New(3)

	for i := range 10 {
		b.InsertInt(i, []byte(fmt.Sprintf("Value for %d", i)))
	}

	ite, err := b.Seek(convertIntToByte(5))
	assert.NoError(t, err)
	assert.NotNil(t, ite)

	assert.Equal(t, []byte("Value for 5"), ite.Value())
	assert.Equal(t, convertBytetoInt(ite.Key()), 5)
}

func TestSeek_Next(t *testing.T) {
	b := New(3)

	for i := range 10 {
		b.InsertInt(i, []byte(fmt.Sprintf("Value for %d", i)))
	}

	ite, err := b.Seek(convertIntToByte(5))
	assert.NoError(t, err)
	assert.NotNil(t, ite)

	assert.Equal(t, []byte("Value for 5"), ite.Value())
	assert.Equal(t, convertBytetoInt(ite.Key()), 5)

	ite.Next()
	assert.Equal(t, []byte("Value for 6"), ite.Value())
	assert.Equal(t, convertBytetoInt(ite.Key()), 6)
}

func TestSeek_Prev(t *testing.T) {
	b := New(3)

	for i := range 10 {
		b.InsertInt(i, []byte(fmt.Sprintf("Value for %d", i)))
	}

	ite, err := b.Seek(convertIntToByte(5))
	assert.NoError(t, err)
	assert.NotNil(t, ite)

	assert.Equal(t, []byte("Value for 5"), ite.Value())
	assert.Equal(t, convertBytetoInt(ite.Key()), 5)

	ite.Prev()
	assert.Equal(t, []byte("Value for 4"), ite.Value())
	assert.Equal(t, convertBytetoInt(ite.Key()), 4)
}

func TestSeekFirst(t *testing.T) {
	b := New(3)

	for i := range 10 {
		b.InsertInt(i, []byte(fmt.Sprintf("Value for %d", i)))
	}

	ite := b.SeekFirst()
	assert.NotNil(t, ite)

	assert.Equal(t, []byte("Value for 0"), ite.Value())
	assert.Equal(t, convertBytetoInt(ite.Key()), 0)

	ite.Prev()
	assert.Nil(t, ite.Value())
	assert.Nil(t, ite.Key())

	// calling iterator.Prev() on leftmost key, makes the iterator invalid
	assert.False(t, ite.Valid())
}

func TestSeekLast(t *testing.T) {
	b := New(3)

	for i := range 10 {
		b.InsertInt(i, []byte(fmt.Sprintf("Value for %d", i)))
	}

	ite := b.SeekLast()
	assert.NotNil(t, ite)

	assert.Equal(t, []byte("Value for 9"), ite.Value())
	assert.Equal(t, convertBytetoInt(ite.Key()), 9)

	ite.Next()
	assert.Nil(t, ite.Value())
	assert.Nil(t, ite.Key())

	// calling iterator.Next() on rightmost key, makes the iterator invalid
	assert.False(t, ite.Valid())
}

func TestSeek_NonExistentKey(t *testing.T) {
	b := New(3)
	for i := 0; i < 10; i += 2 { // Insert 0, 2, 4, 6, 8
		b.InsertInt(i, []byte(fmt.Sprintf("Value for %d", i)))
	}

	// Seek 5 should land on 6 (first key >= 5)
	ite, err := b.Seek(convertIntToByte(5))
	assert.NoError(t, err)
	assert.True(t, ite.Valid())
	assert.Equal(t, 6, convertBytetoInt(ite.Key()))
}

func TestSeek_PastAllKeys(t *testing.T) {
	b := New(3)
	for i := range 10 {
		b.InsertInt(i, []byte(fmt.Sprintf("Value for %d", i)))
	}

	ite, err := b.Seek(convertIntToByte(100))
	assert.NoError(t, err)
	assert.False(t, ite.Valid()) // No key >= 100
}

func TestReverseIteration(t *testing.T) {
	b := New(3)
	for i := range 10 {
		b.InsertInt(i, []byte(fmt.Sprintf("Value for %d", i)))
	}

	values := make([]int, 0)
	for ite := b.SeekLast(); ite.Valid(); ite.Prev() {
		values = append(values, convertBytetoInt(ite.Key()))
	}

	assert.Equal(t, []int{9, 8, 7, 6, 5, 4, 3, 2, 1, 0}, values)
}

func TestEmptyTree(t *testing.T) {
	b := New(3)

	ite := b.SeekFirst()
	assert.True(t, ite == nil)
}
