package timequeue

import (
	"time"
)

type Message[T any] struct {
	ID string

	At time.Time

	Priority int64

	Value T

	index int // used for heap removal safety.
}

func (m Message[T]) less(other Message[T]) bool {
	if m.At.Before(other.At) {
		return true
	}
	if other.At.Before(m.At) {
		return false
	}
	return m.Priority < other.Priority
}
