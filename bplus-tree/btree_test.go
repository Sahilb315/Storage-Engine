package bplustree

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInsert(t *testing.T) {
	b := New(3)

	err := b.Insert([]byte("a"), []byte("v0"))
	assert.NoError(t, err)

	err = b.Insert([]byte("b"), []byte("v1"))
	assert.NoError(t, err)
}

func TestGet(t *testing.T) {
	b := New(3)

	// prepare
	_ = b.Insert([]byte("a"), []byte("v0"))
	_ = b.Insert([]byte("b"), []byte("v1"))

	// retrieve existing
	v, err := b.Get([]byte("b"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v1"), v)

	// missing key
	_, err = b.Get([]byte("z"))
	assert.Error(t, err)
}

func TestDelete(t *testing.T) {
	b := New(3)

	// prepare
	_ = b.Insert([]byte("a"), []byte("v0"))
	_ = b.Insert([]byte("b"), []byte("v1"))

	// delete one key
	err := b.Delete([]byte("a"))
	assert.NoError(t, err)

	// ensure deleted
	_, err = b.Get([]byte("a"))
	assert.Error(t, err)

	// ensure other key still exists
	v, err := b.Get([]byte("b"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("v1"), v)
}

// TestRandomizedOperations performs randomized inserts and deletes while
// maintaining a reference map. Verifies tree state matches expected state.
// Change seed to explore different operation sequences.
func TestRandomizedOperations(t *testing.T) {
	seed := int64(42) // Change to time.Now().UnixNano() for random runs
	t.Logf("random seed: %d", seed)
	rnd := rand.New(rand.NewSource(seed))

	b := New(3)
	ref := make(map[string][]byte)

	// prepare a pool of candidate keys
	poolSize := 300
	pool := make([]string, poolSize)
	for i := range poolSize {
		pool[i] = fmt.Sprintf("k%04d", i)
	}

	ops := 600
	for range ops {
		action := rnd.Intn(3) // 0: insert, 1: delete, 2: insert (update)
		k := pool[rnd.Intn(poolSize)]
		kb := []byte(k)

		switch action {
		case 1: // delete
			_, exists := ref[k]
			err := b.Delete(kb)
			if exists {
				// expected to succeed
				assert.NoError(t, err, "expected delete to succeed for key %s", k)
				delete(ref, k)
			} else {
				// expected to fail when deleting non-existent key
				assert.Error(t, err, "expected delete to fail for missing key %s", k)
			}
		default: // insert or update
			v := []byte(fmt.Sprintf("v%d", rnd.Intn(1_000_000)))
			err := b.Insert(kb, v)
			assert.NoError(t, err, "insert failed for key %s", k)
			// record expected value (insert or update)
			ref[k] = v
		}
	}

	// Validate: every key in ref should be retrievable and match value
	for k, want := range ref {
		got, err := b.Get([]byte(k))
		if !assert.NoError(t, err, "expected key %s to exist", k) {
			continue
		}
		assert.Equal(t, want, got, "value mismatch for key %s", k)
	}

	// Validate: keys not present in ref should not be found
	for _, k := range pool {
		if _, ok := ref[k]; !ok {
			_, err := b.Get([]byte(k))
			assert.Error(t, err, "expected key %s to be missing", k)
		}
	}
}
