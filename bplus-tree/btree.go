package bplustree

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"storage-engine/common"
)

type BTree struct {
	root  *Node
	order int
}

type Node struct {
	key      [][]byte
	value    [][]byte // only if node is leaf node
	children []*Node  // only if node is internal / root node

	// maintain a doubly linked list
	next *Node // only if node is leaf node
	prev *Node
}

func (n *Node) IsLeaf() bool {
	return len(n.children) == 0
}

func New(order int) *BTree {
	common.Assert(order > 0, "order must be positive, got %d", order)
	return &BTree{order: order}
}

func (b *BTree) Insert(key []byte, value []byte) error {
	if b.root == nil {
		root := &Node{
			key:      make([][]byte, 0),
			value:    make([][]byte, 0),
			children: make([]*Node, 0),
		}

		root.key = append(root.key, key)
		root.value = append(root.value, value)

		b.root = root

		return nil
	}

	curr := b.root
	path := make([]*Node, 0)

	for curr != nil && !curr.IsLeaf() {
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
		if b.checkMaxKeys(len(curr.key)) {
			// if max keys, split (recursive process till parent is also not overflowed with keys)
			_, _ = b.splitNode(curr, path)
			return nil
		}
	}
	return nil
}

func (b *BTree) Get(key []byte) ([]byte, error) {
	if b.root == nil {
		return nil, fmt.Errorf("tree is empty")
	}

	n := b.root

	for n != nil && !n.IsLeaf() {
		n = b.traverseRightOrLeft(n, key)
	}

	idx, err := b.findEqualKeyIndexInNode(n, key)

	if err != nil {
		return nil, fmt.Errorf("no key found")
	}

	return n.value[idx], nil
}

func (b *BTree) Delete(key []byte) error {
	if b.root == nil {
		return fmt.Errorf("tree is empty")
	}

	curr := b.root
	path := make([]*Node, 0)

	for curr != nil && !curr.IsLeaf() {
		path = append(path, curr)
		curr = b.traverseRightOrLeft(curr, key)
	}
	if curr == nil {
		return fmt.Errorf("could not find key")
	}

	deleteIdx, err := b.findEqualKeyIndexInNode(curr, key)
	if err != nil {
		return fmt.Errorf("no equal key index found")
	}

	curr.key = append(curr.key[:deleteIdx], curr.key[deleteIdx+1:]...)
	curr.value = append(curr.value[:deleteIdx], curr.value[deleteIdx+1:]...)

	// check if the leaf node is underflowed
	if !b.checkMinKeys(len(curr.key)) {
		_ = b.handleNodeUnderflow(curr, path)
	}
	return nil
}

// Convenience helpers that encode integer keys using fixed-width big-endian
func (b *BTree) InsertInt(k int, value []byte) error {
	return b.Insert(convertIntToByte(k), value)
}

func (b *BTree) GetInt(k int) ([]byte, error) {
	return b.Get(convertIntToByte(k))
}

func (b *BTree) DeleteInt(k int) error {
	return b.Delete(convertIntToByte(k))
}

func (b *BTree) handleNodeUnderflow(node *Node, path []*Node) error {
	common.Assert(node != nil, "handleNodeUnderflow called with nil node")

	var parent *Node
	if len(path) != 0 {
		parent = path[len(path)-1]
	}

	if parent == nil {
		if node == b.root && len(node.key) == 0 && !node.IsLeaf() {
			common.Assert(len(node.children) == 1,
				"collapsing root with 0 keys should have exactly 1 child, got %d",
				len(node.children))
			b.root = node.children[0]
		}
		return nil
	}

	currChildNodeIndex := b.getChildIndexFromParentChildren(parent, node)
	common.Assert(currChildNodeIndex >= 0,
		"node not found in parent's children during underflow handling")
	if currChildNodeIndex < 0 {
		return fmt.Errorf("could not find key: invalid currChildIndex")
	}

	var leftSibling *Node
	var rightSibling *Node

	if currChildNodeIndex > 0 {
		leftSibling = parent.children[currChildNodeIndex-1]
	}
	if currChildNodeIndex < len(parent.children)-1 {
		rightSibling = parent.children[currChildNodeIndex+1]
	}

	// try borrowing from siblings
	if leftSibling != nil && b.checkMinKeys(len(leftSibling.key)) {
		if !node.IsLeaf() {
			node = b.borrowKeyFromINode(leftSibling, node, parent, true)
		} else {
			node = b.borrowKeyFromLeafNode(leftSibling, node, true, parent, currChildNodeIndex)
		}
	} else if rightSibling != nil && b.checkMinKeys(len(rightSibling.key)) {
		if !node.IsLeaf() {
			node = b.borrowKeyFromINode(rightSibling, node, parent, false)
		} else {
			node = b.borrowKeyFromLeafNode(rightSibling, node, false, parent, currChildNodeIndex)
		}
	} else {
		// not able to borrow; merge
		if leftSibling != nil {
			separatorKeyIdxToRemove := currChildNodeIndex - 1
			separatorKey := parent.key[separatorKeyIdxToRemove]
			leftSibling = b.mergeNodes(node, leftSibling, true, separatorKey)
			parent.key = append(parent.key[:separatorKeyIdxToRemove], parent.key[separatorKeyIdxToRemove+1:]...)
		} else {
			separatorKeyIdxToRemove := currChildNodeIndex
			separatorKey := parent.key[separatorKeyIdxToRemove]
			parent.key = append(parent.key[:separatorKeyIdxToRemove], parent.key[separatorKeyIdxToRemove+1:]...)
			rightSibling = b.mergeNodes(node, rightSibling, false, separatorKey)
		}
		// after merging nodes, only one node is required. we do not require the other child which was the src
		parent.children = append(parent.children[:currChildNodeIndex], parent.children[currChildNodeIndex+1:]...)

		// Update parent separators to reflect current state of children after merge
		// Only needed for leaf children; internal node children have correct separators
		if len(parent.children) > 0 && parent.children[0].IsLeaf() {
			for i := 0; i < len(parent.key); i++ {
				if i+1 < len(parent.children) && len(parent.children[i+1].key) > 0 {
					// parent.key[i] = first key of parent.children[i+1]
					parent.key[i] = parent.children[i+1].key[0]
				}
			}
		}
	}

	if !b.checkMinKeys(len(parent.key)) {
		// check underflow for internal nodes
		_ = b.handleNodeUnderflow(parent, path[:len(path)-1])
	}

	return nil
}

// dst is the node where the merge happens i.e. the node which satisfies the min keys criteria
// src is the underflowed node which merges with the `dst` node
// separatorKey is the key from the parent that separates src and dst (needed for internal nodes)
func (b *BTree) mergeNodes(src, dst *Node, mergeWithLeft bool, separatorKey []byte) *Node {
	common.Assert(src.IsLeaf() == dst.IsLeaf(),
		"mergeNodes called with mismatched node types (src.IsLeaf=%v, dst.IsLeaf=%v)",
		src.IsLeaf(), dst.IsLeaf())

	isInternalNode := !src.IsLeaf() || !dst.IsLeaf()

	if mergeWithLeft {
		// dst is left sibling, src is the underflowed node (to the right)
		if isInternalNode {
			// For internal nodes: include separator key between dst and src keys
			dst.key = append(dst.key, separatorKey)
			dst.key = append(dst.key, src.key...)
			dst.children = append(dst.children, src.children...)
		} else {
			// For leaf nodes: just concatenate (separator is copy-up, not stored)
			dst.key = append(dst.key, src.key...)
			dst.value = append(dst.value, src.value...)
			// Update the next pointer: dst now points to what src pointed to
			dst.next = src.next

			// update the prev pointer of the next node
			if dst.next != nil {
				dst.next.prev = dst
			}
		}

		return dst
	} else {
		// dst is right sibling, src is the underflowed node (to the left)
		if isInternalNode {
			// For internal nodes: include separator key between src and dst keys
			newKeys := make([][]byte, 0, len(src.key)+1+len(dst.key))
			newKeys = append(newKeys, src.key...)
			newKeys = append(newKeys, separatorKey)
			newKeys = append(newKeys, dst.key...)

			dst.key = newKeys
			dst.children = append(src.children, dst.children...)
		} else {
			// For leaf nodes: just concatenate
			dst.key = append(src.key, dst.key...)
			dst.value = append(src.value, dst.value...)

			if src.prev != nil {
				src.prev.next = dst
			}

			dst.prev = src.prev
		}

		return dst
	}
}

// src is the node from which the KV is borrowed from
// dst is the underflowed node which borrows a KV from `src`.
// parent is the parent node, dstIdx is the index of dst in parent.children
func (b *BTree) borrowKeyFromLeafNode(src, dst *Node, borrowFromLeft bool, parent *Node, dstIdx int) *Node {
	common.Assert(src.IsLeaf() && dst.IsLeaf(),
		"borrowKeyFromLeafNode called with non-leaf nodes (src.IsLeaf=%v, dst.IsLeaf=%v)",
		src.IsLeaf(), dst.IsLeaf())
	common.Assert(len(src.key) > 0, "cannot borrow from empty source node")
	common.Assert(parent != nil, "parent cannot be nil when borrowing")

	// borrow from the left sibling i.e. get the rightmost key
	if borrowFromLeft {
		lastIdx := len(src.key) - 1
		lastKey := src.key[lastIdx]
		lastVal := src.value[lastIdx]

		// remove the last KV from the source / borrower
		src.key = src.key[:len(src.key)-1]
		src.value = src.value[:len(src.value)-1]

		// prepend the KV into the dst
		dst.key = append([][]byte{lastKey}, dst.key...)
		dst.value = append([][]byte{lastVal}, dst.value...)

		// update separator: dst's first key changed
		parent.key[dstIdx-1] = dst.key[0]

		return dst
	} else { // borrow from the right sibling i.e. get the leftmost key
		firstKey := src.key[0]
		firstVal := src.value[0]

		// remove the first KV from the source / borrower
		src.key = src.key[1:]
		src.value = src.value[1:]

		// append the KV into the dst
		dst.key = append(dst.key, firstKey)
		dst.value = append(dst.value, firstVal)

		// update separator: src's first key changed
		parent.key[dstIdx] = src.key[0]

		return dst
	}
}

func (b *BTree) borrowKeyFromINode(src, dst, parent *Node, borrowFromLeft bool) *Node {
	common.Assert(!src.IsLeaf() && !dst.IsLeaf(),
		"borrowKeyFromINode called with leaf nodes (src.IsLeaf=%v, dst.IsLeaf=%v)",
		src.IsLeaf(), dst.IsLeaf())
	common.Assert(len(src.key) > 0, "cannot borrow from empty source node")
	common.Assert(len(src.children) > 0, "source internal node has no children")
	common.Assert(parent != nil, "parent cannot be nil when borrowing")

	idx := b.getChildIndexFromParentChildren(parent, dst)
	common.Assert(idx >= 0, "dst node not found in parent's children")

	if borrowFromLeft {
		separatorKey := parent.key[idx-1]

		// prepend the Key to the dst node
		dst.key = append([][]byte{separatorKey}, dst.key...)
		dst.children = append([]*Node{src.children[len(src.children)-1]}, dst.children...)

		// promote the sibling key to its parent
		keyToBePromoted := src.key[len(src.key)-1]

		// remove the key from the sibling node
		src.key = src.key[:len(src.key)-1]
		src.children = src.children[:len(src.children)-1]

		parent.key[idx-1] = keyToBePromoted
		return dst
	} else {
		separatorKey := parent.key[idx]

		// append the Key to the dst node
		dst.key = append(dst.key, separatorKey)
		dst.children = append(dst.children, src.children[0])

		// promote the sibling key to its parent
		keyToBePromoted := src.key[0]

		// remove the key from the sibling node
		src.key = src.key[1:]
		src.children = src.children[1:]

		parent.key[idx] = keyToBePromoted
		return dst
	}
}

func (b *BTree) getChildIndexFromParentChildren(parent, child *Node) int {
	if parent == nil || child == nil {
		return -1
	}

	for i, c := range parent.children {
		if c == child {
			return i
		}
	}

	return -1
}

func (b *BTree) findEqualKeyIndexInNode(node *Node, key []byte) (int, error) {
	for i, k := range node.key {
		if bytes.Equal(k, key) {
			return i, nil
		}
	}

	return 0, fmt.Errorf("no key found")
}

func (b *BTree) splitNode(node *Node, path []*Node) (left, right *Node) {
	common.Assert(node != nil, "cannot split nil node")
	common.Assert(len(node.key) > 2*b.order,
		"splitNode called but node only has %d keys (need >%d to split)",
		len(node.key), 2*b.order)

	// leaf node splitting
	if node.IsLeaf() {
		common.Assert(len(node.key) == len(node.value),
			"leaf node key/value mismatch before split: %d keys, %d values",
			len(node.key), len(node.value))

		right = &Node{}
		numRightKeys := len(node.key) - b.order
		right.key = make([][]byte, numRightKeys)
		right.value = make([][]byte, numRightKeys)

		left = node

		// copy the order+1 KV to the new node
		for i := range numRightKeys {
			right.key[i] = left.key[b.order+i]
			right.value[i] = left.value[b.order+i]
		}

		right.next = left.next
		left.next = right

		right.prev = left

		if right.next != nil {
			// update the prev pointer of the next node
			right.next.prev = right
		}

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
		if b.checkMaxKeys(len(parent.key)) {
			return b.splitNode(parent, path[:len(path)-1])
		}
		return
	} else {
		// Internal node split
		common.Assert(len(node.children) == len(node.key)+1,
			"internal node children/key mismatch before split: %d children, %d keys",
			len(node.children), len(node.key))

		right = &Node{}

		// Calculate how many keys go to right (all keys after the separator)
		numRightKeys := len(node.key) - b.order - 1
		numRightChildren := len(node.children) - b.order - 1

		right.key = make([][]byte, numRightKeys)
		right.children = make([]*Node, numRightChildren)

		left = node

		// Copy keys and children to right node
		for i := range numRightKeys {
			right.key[i] = left.key[b.order+1+i]
		}
		for i := range numRightChildren {
			right.children[i] = left.children[b.order+1+i]
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

		if b.checkMaxKeys(len(parent.key)) {
			return b.splitNode(parent, path[:len(path)-1])
		}
		return
	}
}

func (b *BTree) insertKeyInNodeInPlace(node *Node, key []byte, childPtr *Node, indexToInsert int) {
	common.Assert(!node.IsLeaf(), "insertKeyInNodeInPlace called on leaf node")
	common.Assert(indexToInsert >= 0 && indexToInsert <= len(node.key),
		"insertion index %d out of bounds [0, %d]", indexToInsert, len(node.key))
	common.Assert(childPtr != nil, "childPtr cannot be nil for internal node insertion")

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
	val []byte,
	indexToInsert int,
) {
	common.Assert(node.IsLeaf(), "insertKVInLeafInPlace called on internal node")
	common.Assert(indexToInsert >= 0 && indexToInsert <= len(node.key),
		"insertion index %d out of bounds [0, %d]", indexToInsert, len(node.key))
	common.Assert(len(node.key) == len(node.value),
		"leaf node key/value length mismatch: %d keys, %d values", len(node.key), len(node.value))

	node.key = append(node.key, nil)
	node.value = append(node.value, nil)

	// Shift keys and values to the right
	copy(node.key[indexToInsert+1:], node.key[indexToInsert:])
	copy(node.value[indexToInsert+1:], node.value[indexToInsert:])

	node.key[indexToInsert] = key
	node.value[indexToInsert] = val
}

func (b *BTree) checkMaxKeys(keysLen int) bool {
	return keysLen > 2*b.order
}

func (b *BTree) checkMinKeys(keysLen int) bool {
	return keysLen > b.order
}

func (b *BTree) traverseRightOrLeft(node *Node, key []byte) *Node {
	if node == nil {
		return nil
	}

	// Internal node invariant: must have exactly len(keys)+1 children
	common.Assert(len(node.children) == len(node.key)+1,
		"internal node has %d children but %d keys (expected %d children)",
		len(node.children), len(node.key), len(node.key)+1)

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

// PrettyPrint prints the B+tree in a hierarchical format
func (b *BTree) PrettyPrint() {
	if b.root == nil {
		fmt.Println("(empty tree)")
		return
	}
	b.printNode(b.root, "", true)
}

func (b *BTree) printNode(node *Node, prefix string, isLast bool) {
	if node == nil {
		return
	}

	// Connector for tree branches
	connector := "├── "
	if isLast {
		connector = "└── "
	}

	// Determine node type label
	label := "INTERNAL"
	if node.IsLeaf() {
		label = "LEAF"
	} else if node == b.root {
		label = "ROOT"
	}

	// Print keys
	fmt.Printf("%s%s%s [", prefix, connector, label)
	for i, key := range node.key {
		if i > 0 {
			fmt.Print(", ")
		}
		if node.IsLeaf() {
			// Leaf: show key:value
			fmt.Printf("%s:%s", string(key), string(node.value[i]))
		} else {
			// Internal: just show key
			fmt.Printf("%s", string(key))
		}
	}
	fmt.Println("]")

	// Recurse into children
	childPrefix := prefix
	if isLast {
		childPrefix += "    "
	} else {
		childPrefix += "│   "
	}
	for i, child := range node.children {
		b.printNode(child, childPrefix, i == len(node.children)-1)
	}
}

func convertIntToByte(i int) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(i))
	return buf[:]
}

func convertBytetoInt(b []byte) int {
	if len(b) < 8 {
		// normalize shorter slices by left-padding to 8 bytes
		var tmp [8]byte
		copy(tmp[8-len(b):], b)
		return int(binary.BigEndian.Uint64(tmp[:]))
	}
	return int(binary.BigEndian.Uint64(b))
}
