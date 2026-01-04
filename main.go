package main

import (
	"bplus-tree/btree"
	"fmt"
)

func main() {
	btree := btree.New(2)

	btree.Insert([]byte("10"), "This is a pointer to 10")
	btree.Insert([]byte("11"), "This is a pointer to 11")
	btree.Insert([]byte("12"), "This is a pointer to 11")
	btree.Insert([]byte("120"), "This is a pointer to 11")
	btree.Insert([]byte("sa"), "sahil")
	btree.Insert([]byte("11"), "This is a pointer to 11211")
	btree.Insert([]byte("1"), "This is a pointer to 1")

	v, _ := btree.Get([]byte("sa"))
	fmt.Println(v)
}
