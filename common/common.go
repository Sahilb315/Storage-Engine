package common

import "fmt"

// Assert panics with a formatted message if the given condition is false.
func Assert(condition bool, msg string, v ...any) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}
