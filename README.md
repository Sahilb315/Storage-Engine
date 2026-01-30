# storage-engine

A storage engine built from scratch in Go. Learning project - implementing database internals from first principles.

## Current Status

**B+ Tree** - Done (in-memory)
- Insert, Get, Delete
- Node splitting and merging
- Borrowing from siblings
- Tested with randomized operations

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
│   ├── btree.go       # B+ tree implementation
│   └── btree_test.go  # Tests
├── main.go            # Playground for testing
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
