package btree

import (
	"bytes"
	"fmt"
)

type BTree struct {
	root  *Node
	order int
}

type Node struct {
	key      [][]byte
	value    []string // only if node is leaf node
	children []*Node  // only if node is internal / root node
	next     *Node    // only if node is leaf node
}

func New(order int) *BTree {
	return &BTree{order: order}
}

func (b *BTree) Insert(key []byte, value string) error {
	if b.root == nil {
		root := &Node{
			key:      make([][]byte, 0),
			value:    make([]string, 0),
			children: make([]*Node, 0),
		}

		root.key = append(root.key, key)
		root.value = append(root.value, value)

		b.root = root

		return nil
	}

	curr := b.root
	path := make([]*Node, 0)

	for curr != nil && len(curr.children) != 0 {
		path = append(path, curr)
		curr = b.traverseRightOrLeft(curr, key)
	}

	kvInsertionIndex := b.findKeyIndexInNode(curr, key)
	if kvInsertionIndex == -1 {
		return fmt.Errorf("failed to insert key")
	}

	if len(curr.key) > kvInsertionIndex && bytes.Equal(curr.key[kvInsertionIndex], key) {
		// key exists, update the value
		curr.value[kvInsertionIndex] = value
	} else {
		// append the key value to the insertion index
		b.insertKVInLeafInPlace(curr, key, value, kvInsertionIndex)
		// check if the lead node has max keys
		if b.checkOrder(len(curr.key)) {
			// if max keys, split (recursive process till parent is also not overflowed with keys)
			_, _ = b.splitNode(curr, path)
			return nil
		}
	}
	return nil
}

func (b *BTree) Get(key []byte) (string, error) {
	n := b.root

	for n != nil && len(n.children) != 0 {
		n = b.traverseRightOrLeft(n, key)
	}

	idx := b.findKeyIndexInNode(n, key)

	return n.value[idx], nil
}

func (b *BTree) splitNode(node *Node, path []*Node) (left, right *Node) {
	childrenLen := len(node.children)

	if childrenLen == 0 {
		right = &Node{}
		right.key = make([][]byte, b.order+1)
		right.value = make([]string, b.order+1)

		left = node

		// copy the order+1 KV to the new node
		for i := 0; i <= b.order; i++ {
			right.key[i] = left.key[b.order+i]
			right.value[i] = left.value[b.order+i]
		}

		right.next = left.next
		left.next = right

		left.key = left.key[:b.order]
		left.value = left.value[:b.order]

		separatorKey := right.key[0]

		var parent *Node
		if len(path) != 0 {
			parent = path[len(path)-1]
		}
		if parent == nil {
			// create a new root
			newRoot := &Node{}
			newRoot.key = append(newRoot.key, separatorKey)
			newRoot.children = append(newRoot.children, left, right)

			b.root = newRoot
			return
		}
		insertionIdx := b.findKeyIndexInNode(parent, separatorKey)
		b.insertKeyInNodeInPlace(parent, separatorKey, right, insertionIdx)
		if b.checkOrder(len(parent.key)) {
			return b.splitNode(parent, path[:len(path)-1])
		}
		return
	} else {
		right = &Node{}
		right.key = make([][]byte, b.order)
		right.children = make([]*Node, b.order+1)

		left = node

		for i := 0; i < b.order; i++ {
			right.key[i] = left.key[b.order+i+1]
		}
		for i := 0; i < b.order+1; i++ {
			right.children[i] = left.children[b.order+i+1]
		}

		separatorKey := left.key[b.order]

		left.key = left.key[:b.order]
		left.children = left.children[:b.order+1]

		var parent *Node
		if len(path) != 0 {
			parent = path[len(path)-1]
		}
		if parent == nil {
			// create a new root
			newRoot := &Node{}
			newRoot.key = append(newRoot.key, separatorKey)
			newRoot.children = append(newRoot.children, left, right)

			b.root = newRoot
			return
		}
		insertionIdx := b.findKeyIndexInNode(parent, separatorKey)
		b.insertKeyInNodeInPlace(parent, separatorKey, right, insertionIdx)
		if b.checkOrder(len(parent.key)) {
			return b.splitNode(parent, path[:len(path)-1])
		}
		return
	}
}

func (b *BTree) insertKeyInNodeInPlace(node *Node, key []byte, childPtr *Node, indexToInsert int) {
	node.key = append(node.key, nil)
	node.children = append(node.children, nil)

	// Shift keys and children to the right
	copy(node.key[indexToInsert+1:], node.key[indexToInsert:])
	copy(node.children[indexToInsert+1+1:], node.children[indexToInsert+1:])

	node.key[indexToInsert] = key
	node.children[indexToInsert+1] = childPtr
}

func (b *BTree) insertKVInLeafInPlace(
	node *Node,
	key []byte,
	val string,
	indexToInsert int,
) {
	node.key = append(node.key, nil)
	node.value = append(node.value, "")

	// Shift keys and values to the right
	copy(node.key[indexToInsert+1:], node.key[indexToInsert:])
	copy(node.value[indexToInsert+1:], node.value[indexToInsert:])

	node.key[indexToInsert] = key
	node.value[indexToInsert] = val
}

func (b *BTree) checkOrder(keysLen int) bool {
	return keysLen > 2*b.order
}

func (b *BTree) traverseRightOrLeft(node *Node, key []byte) *Node {
	if node == nil {
		return nil
	}

	for i, v := range node.key {
		if bytes.Compare(key, v) < 0 {
			return node.children[i]
		}
	}

	return node.children[len(node.key)]
}

func (b *BTree) findKeyIndexInNode(node *Node, key []byte) int {
	if node == nil {
		return -1
	}

	for i, v := range node.key {
		c := bytes.Compare(key, v)
		if c <= 0 {
			return i
		}
	}

	return len(node.key)
}
