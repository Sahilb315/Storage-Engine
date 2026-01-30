package main

import (
	"fmt"
	bplustree "storage-engine/bplus-tree"
	"strconv"
)

func main() {
	btree := bplustree.New(3)
	poolSize := 300
	for i := range poolSize {
		key := fmt.Sprintf("k%04d", i)
		btree.Insert([]byte(key), strconv.Itoa(i))
	}
	btree.PrettyPrint()

	for i := 0; i < poolSize; i += 5 {
		key := fmt.Sprintf("k%04d", i)
		err := btree.Delete([]byte(key))

		if err != nil {
			fmt.Printf("Error while deleting: %s with error: %s\n", key, err.Error())
		}
	}
	v, err := btree.Get([]byte("k0296"))
	if err != nil {
		fmt.Println("Error while getting value: ", err)
	}
	fmt.Println("Value of key: ", v)
}
