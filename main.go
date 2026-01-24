package main

import (
	"fmt"
	bplustree "storage-engine/bplus-tree"
)

func main() {
	btree := bplustree.New(3)

	btree.Insert([]byte("10"), "10")
	btree.Insert([]byte("11"), "11")
	btree.Insert([]byte("12"), "11")
	btree.Insert([]byte("120"), "11")
	btree.Insert([]byte("sa"), "sahil")
	btree.Insert([]byte("13"), "11211")
	btree.Insert([]byte("1"), "1")

	btree.PrettyPrint()
	v, _ := btree.Get([]byte("1"))
	fmt.Println("Value of key 1:", v)

	err := btree.Delete([]byte("1"))
	if err != nil {
		fmt.Println("Error while deleting: ", err.Error())
	}
	btree.PrettyPrint()

	v, err = btree.Get([]byte("1"))
	if err != nil {
		fmt.Println("error while retrieving value: ", err)
	}
	if v != "" {
		fmt.Println("Value of key 1 after deleting:", v)
	}
}
