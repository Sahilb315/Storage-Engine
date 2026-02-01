package bplustree

import (
	"bytes"
	"fmt"
)

type iterator struct {
	node *Node // the node iterator points to
	idx  int   // the index of the key in the node
}

func (b *BTree) Seek(key []byte) (*iterator, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("got empty key")
	}

	if b.root == nil {
		return nil, fmt.Errorf("empty tree")
	}

	n := b.root

	for n != nil && !n.IsLeaf() {
		n = b.traverseRightOrLeft(n, key)
	}

	idx := 0

	// Find first key >= search key
	for idx < len(n.key) && bytes.Compare(n.key[idx], key) < 0 {
		idx++
	}

	// Past end of this leaf, move to next
	if idx >= len(n.key) {
		return &iterator{node: n.next, idx: 0}, nil
	}

	return &iterator{node: n, idx: idx}, nil
}

func (b *BTree) SeekFirst() *iterator {
	if b.root == nil {
		return nil
	}

	n := b.root

	for n != nil && !n.IsLeaf() {
		n = n.children[0]
	}

	idx := 0
	return &iterator{node: n, idx: idx}
}

func (b *BTree) SeekLast() *iterator {
	if b.root == nil {
		return nil
	}

	n := b.root

	for n != nil && !n.IsLeaf() {
		n = n.children[len(n.children)-1]
	}

	idx := len(n.key) - 1
	return &iterator{node: n, idx: idx}
}

func (i *iterator) Next() {
	if !i.Valid() {
		return
	}

	if i.idx+1 < len(i.node.key) {
		i.idx++
	} else {
		if i.node.next != nil {
			i.node = i.node.next
			i.idx = 0
		} else {
			i.node = nil
		}
	}
}

func (i *iterator) Prev() {
	if !i.Valid() {
		return
	}

	if i.idx-1 >= 0 {
		i.idx--
	} else {
		if i.node.prev != nil {
			i.node = i.node.prev
			i.idx = len(i.node.key) - 1
		} else {
			i.node = nil
		}
	}
}

func (i *iterator) Key() []byte {
	if !i.Valid() {
		return nil
	}

	return i.node.key[i.idx]
}

func (i *iterator) Value() string {
	if !i.Valid() {
		return ""
	}
	return i.node.value[i.idx]
}

func (i *iterator) Valid() bool {
	return i.node != nil && i.idx >= 0 && i.idx < len(i.node.key)
}
