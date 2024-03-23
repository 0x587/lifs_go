package kv

import "fmt"

type NotFoundError struct {
	Key []byte
}

var _ error = NotFoundError{}

func (n NotFoundError) Error() string {
	return fmt.Sprintf("Not found: %x", n.Key)
}
