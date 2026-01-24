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
		if b.checkMaxKeys(len(curr.key)) {
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

	idx := -1
	for i, k := range n.key {
		if bytes.Equal(k, key) {
			idx = i
		}
	}

	if idx == -1 {
		return "", fmt.Errorf("no key found")
	}

	return n.value[idx], nil
}

func (b *BTree) Delete(key []byte) error {
	curr := b.root
	path := make([]*Node, 0)

	for curr != nil && len(curr.children) != 0 {
		path = append(path, curr)
		curr = b.traverseRightOrLeft(curr, key)
	}

	deleteIdx := b.findKeyIndexInNode(curr, key)

	if deleteIdx < 0 {
		return fmt.Errorf("could not find key")
	}

	curr.key = append(curr.key[:deleteIdx], curr.key[deleteIdx+1:]...)
	curr.value = append(curr.value[:deleteIdx], curr.value[deleteIdx+1:]...)

	// check if the leaf node is underflowed
	if !b.checkMinKeys(len(curr.key)) {
		var parent *Node
		if len(path) != 0 {
			parent = path[len(path)-1]
		}
		if parent == nil {
			return fmt.Errorf("could not find key: invalid parent")
		}

		currChildNodeIndex := b.getChildIndexFromParentChildren(parent, curr)
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
			curr = b.borrowKeyFromLeafNode(leftSibling, curr, true)
		} else if rightSibling != nil && b.checkMinKeys(len(rightSibling.key)) {
			curr = b.borrowKeyFromLeafNode(rightSibling, curr, false)
		} else {
			// not able to borrow; merge
			if leftSibling != nil {
				leftSibling = b.mergeLeafNodes(curr, leftSibling, true)
				separatorKeyIdxToRemove := currChildNodeIndex - 1
				parent.key = append(parent.key[:separatorKeyIdxToRemove], parent.key[separatorKeyIdxToRemove+1:]...)
			} else {
				separatorKeyIdxToRemove := currChildNodeIndex
				parent.key = append(parent.key[:separatorKeyIdxToRemove], parent.key[separatorKeyIdxToRemove+1:]...)
				rightSibling = b.mergeLeafNodes(curr, rightSibling, false)
			}
			parent.children = append(parent.children[:currChildNodeIndex], parent.children[currChildNodeIndex+1:]...)
		}

		// update the parent separator
		for i := 0; i < len(parent.key); i++ {
			parent.key[i] = parent.children[i+1].key[0]
		}
		return nil
	}
	return nil
}

func (b *BTree) mergeLeafNodes(src, dst *Node, mergeWithLeft bool) *Node {
	if mergeWithLeft {
		dst.key = append(dst.key, src.key...)
		dst.value = append(dst.value, src.value...)

		return dst
	} else {
		dst.key = append(src.key, dst.key...)
		dst.value = append(src.value, dst.value...)
		return dst
	}
}

func (b *BTree) borrowKeyFromLeafNode(src, dst *Node, borrowFromLeft bool) *Node {
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
		dst.value = append([]string{lastVal}, dst.value...)

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
		if b.checkMaxKeys(len(parent.key)) {
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
		if b.checkMaxKeys(len(parent.key)) {
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
		fmt.Println("Empty tree")
		return
	}

	fmt.Println("B+Tree Structure:")
	fmt.Println("==================")
	b.printNode(b.root, "", true, 0)

	// Also print leaf level traversal
	// fmt.Println("\nLeaf Level Traversal:")
	// fmt.Println("=====================")
	// b.printLeafTraversal()
}

func (b *BTree) printNode(node *Node, prefix string, isLast bool, level int) {
	if node == nil {
		return
	}

	connector := "├── "
	if isLast {
		connector = "└── "
	}

	fmt.Printf("%s%s", prefix, connector)

	// Print node type and keys
	if len(node.children) == 0 {
		// Leaf node
		fmt.Printf("LEAF [")
		for i, key := range node.key {
			if i > 0 {
				fmt.Printf(", ")
			}
			fmt.Printf("%s:%s", string(key), node.value[i])
		}
		fmt.Printf("]\n")
	} else {
		// Internal node
		fmt.Printf("INTERNAL [")
		for i, key := range node.key {
			if i > 0 {
				fmt.Printf(", ")
			}
			fmt.Printf("%s", string(key))
		}
		fmt.Printf("]\n")
	}

	newPrefix := prefix
	if isLast {
		newPrefix += "    "
	} else {
		newPrefix += "│   "
	}

	// Recursively print children for internal nodes
	if len(node.children) > 0 {
		for i, child := range node.children {
			isLastChild := i == len(node.children)-1
			b.printNode(child, newPrefix, isLastChild, level+1)
		}
	}
}

func (b *BTree) printLeafTraversal() {
	if b.root == nil {
		fmt.Println("Empty tree")
		return
	}

	// Find the leftmost leaf node
	current := b.root
	for current != nil && len(current.children) > 0 {
		current = current.children[0]
	}

	// Traverse leaf nodes using next pointers
	fmt.Print("Keys: ")
	first := true
	for current != nil {
		for i, key := range current.key {
			if !first {
				fmt.Print(" -> ")
			}
			fmt.Printf("[%s:%s]", string(key), current.value[i])
			first = false
		}
		current = current.next
	}
	fmt.Println()
}
