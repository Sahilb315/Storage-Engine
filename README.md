# storage-engine

A storage engine built from scratch in Go. Learning project - implementing database internals from first principles.

> Trying to keep the project as raw as possible, there could be rough or missing edge cases.

## Current Status

**B+ Tree** - Done (in-memory)
- Insert, Get, Delete
- Iterator with Seek, SeekFirst, SeekLast, Next, Prev

## What's Next

- [ ] Page-based storage (fixed-size pages, disk persistence)
- [ ] Buffer pool / page cache
- [ ] Write-ahead logging
- [ ] Crash recovery
- [ ] Concurrency (latches, Lehman-Yao)

## Structure

```
storage-engine/
├── bplus-tree/
│   ├── btree.go          # B+ tree implementation
│   ├── iterator.go       # Iterator for range scans
│   ├── btree_test.go     
│   └── iterator_test.go  
├── main.go               # Playground for testing
└── README.md
```

## Usage

```go
import bplustree "storage-engine/bplus-tree"

// Create tree with order 3 (nodes have 3-6 keys)
tree := bplustree.New(3)

// Insert
tree.Insert([]byte("key"), "value")

// Get
val, err := tree.Get([]byte("key"))

// Delete
err := tree.Delete([]byte("key"))

// For integer keys (uses order-preserving encoding)
tree.InsertInt(42, "value")
tree.GetInt(42)
tree.DeleteInt(42)

// Iterator - range scans
iter := tree.SeekFirst()
for iter.Valid() {
    fmt.Println(iter.Key(), iter.Value())
    iter.Next()
}

// Seek to specific key
iter, _ := tree.Seek([]byte("key"))
```

## Running Tests

```bash
go test ./bplus-tree/... -v
```

## Why

Building this to understand how databases actually work - not the theory, but the implementation details that papers gloss over. Things like:

- Why B+ tree deletion is way harder than insertion
- What happens when you crash mid-write
- Why concurrent B+ trees need special techniques (Lehman-Yao)
- How buffer pools and WAL interact

## Notes

This is a learning project. The code prioritizes clarity over performance. There will be bugs, rewrites, and dead ends. That's the point.
