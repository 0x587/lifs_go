package cas

import "fmt"

type NotFoundError struct {
	Type  string
	Level uint8
	Key   Key
}

var _ error = NotFoundError{}

func (n NotFoundError) Error() string {
	return fmt.Sprintf("[ErrCAS] Not found: %q@%d %s", n.Type, n.Level, n.Key)
}
