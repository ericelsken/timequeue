package timequeue

import (
	"container/heap"
	"errors"
	"fmt"
	"time"
)

const notInIndex = -1

var (
	errIDAlreadyInUse = errors.New("timequeue: id already in use")
	errNotInHeap      = errors.New("timequeue: not in heap")
)

func (m *Message[_]) clearHeap() {
	m.index = notInIndex
}

type messageHeap[T any] struct {
	messages []*Message[T]
	byID     map[string]*Message[T]
}

func newMessageHeap[T any]() *messageHeap[T] {
	mh := &messageHeap[T]{
		messages: append([]*Message[T]{}, nil...),
		// TODO initialize the map
		byID: make(map[string]*Message[T]),
	}
	heap.Init(mh)
	return mh
}

func (mh *messageHeap[_]) Len() int {
	return len(mh.messages)
}

func (mh *messageHeap[_]) Less(i, j int) bool {
	return mh.messages[i].less(*mh.messages[j])
}

func (mh *messageHeap[_]) Swap(i, j int) {
	mh.messages[i], mh.messages[j] = mh.messages[j], mh.messages[i]
	mh.messages[i].index = i
	mh.messages[j].index = j
}

func (mh *messageHeap[T]) Push(m any) {
	message := m.(*Message[T])
	message.index = len(mh.messages)
	mh.messages = append(mh.messages, message)
	mh.byID[message.ID] = message
}

func (mh *messageHeap[T]) Pop() any {
	n := len(mh.messages)
	result := mh.messages[n-1]
	result.clearHeap()
	mh.messages[n-1] = nil // avoid memory leak
	mh.messages = mh.messages[:n-1]
	delete(mh.byID, result.ID)
	return result
}

func (mh *messageHeap[T]) peekMessage() *Message[T] {
	if mh.Len() == 0 {
		return nil
	}
	return mh.messages[0]
}

func (mh *messageHeap[T]) pushMessage(id string, at time.Time, priority int64, value T) (*Message[T], error) {
	if _, ok := mh.byID[id]; ok {
		return nil, errIDAlreadyInUse
	}
	message := &Message[T]{
		ID:       id,
		At:       at,
		Priority: priority,
		Value:    value,
	}
	heap.Push(mh, message)
	return message, nil
}

func (mh *messageHeap[T]) popMessage() *Message[T] {
	if mh.Len() == 0 {
		return nil
	}
	return heap.Pop(mh).(*Message[T])
}

func (mh *messageHeap[T]) removeMessage(id string) (*Message[T], error) {
	message, ok := mh.byID[id]
	if !ok {
		return nil, fmt.Errorf("not in heap")
	}
	return heap.Remove(mh, message.index).(*Message[T]), nil
}

func (mh *messageHeap[T]) unload() []*Message[T] {
	result := mh.messages
	for _, m := range mh.messages {
		m.clearHeap()
	}
	mh.messages = nil
	mh.byID = map[string]*Message[T]{}
	return result
}
