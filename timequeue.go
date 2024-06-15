package timequeue

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type TimeQueue[T any] struct {
	timer *time.Timer

	dequeue chan struct{}

	preempt        chan preemptData
	preemptContext context.Context
	preemptCancel  context.CancelFunc

	modify chan struct{}

	messageHeap *messageHeap[T]
}

func New[T any]() *TimeQueue[T] {
	tq := &TimeQueue[T]{
		timer:       time.NewTimer(0),
		dequeue:     make(chan struct{}, 1),
		preempt:     make(chan preemptData, 1), // The size of preempt must be >= 1, but aside from that does not matter.
		modify:      make(chan struct{}, 1),
		messageHeap: newMessageHeap[T](),
	}
	defer tq.setPreemptCancel()
	<-tq.timer.C
	tq.dequeue <- struct{}{}
	tq.modify <- struct{}{}
	return tq
}

func (tq *TimeQueue[T]) Dequeue(ctx context.Context) (message *Message[T], now time.Time, err error) {
	if err = tq.lockDequeue(ctx); err != nil {
		return
	}
	defer tq.unlockDequeue()

	// Have the dequeue lock. There is just one goroutine here.

	now, err = tq.getTimerValue(ctx)
	if err != nil {
		return
	}

	// Have the dequeue lock and an expired timer.

	if err = tq.lockModify(ctx); err != nil {
		return
	}
	defer tq.unlockModify()

	// Have the dequeue lock, expired timer, and modify lock.

	result := tq.messageHeap.popMessage()
	if result == nil {
		err = fmt.Errorf("empty")
		return
	}
	message = result

	// var result time.Time
	// if len(tq.messages) == 0 {
	// 	return time.Time{}, fmt.Errorf("empty")
	// }
	// result = tq.messages[0]
	// tq.messages = tq.messages[1:]

	// TODO reset the timer to the nearest time.
	if earliest := tq.messageHeap.peekMessage(); earliest != nil {
		tq.timer.Reset(time.Until(earliest.At))
	}

	// if len(tq.messages) > 0 {
	// 	earliest := tq.messages[0]
	// 	// tq.resetTimer(earliest)
	// 	tq.timer.Reset(time.Until(earliest))
	// }

	return
}

func (tq *TimeQueue[_]) lockDequeue(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tq.dequeue:
		return nil
	}
}

func (tq *TimeQueue[_]) unlockDequeue() {
	tq.dequeue <- struct{}{}
}

func (tq *TimeQueue[_]) getTimerValue(ctx context.Context) (now time.Time, err error) {
	haveTimerValue := false
	for !haveTimerValue {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return

		case preempted := <-tq.preempt:
			if preempted.hadTimer {
				if !tq.timer.Stop() {
					<-tq.timer.C
				}
			}
			tq.timer.Reset(time.Until(preempted.earliest))

		case now = <-tq.timer.C:
			haveTimerValue = true
		}
	}
	return
}

func (tq *TimeQueue[T]) EnqueueValue(ctx context.Context, value T) (Message[T], error) {
	return tq.Enqueue(ctx, value, time.Now(), 0)
}

func (tq *TimeQueue[T]) Enqueue(ctx context.Context, value T, at time.Time, priority int64) (Message[T], error) {
	id, err := uuid.NewRandom()
	if err != nil {
		return Message[T]{}, err
	}

	if err := tq.lockModify(ctx); err != nil {
		return Message[T]{}, err
	}
	defer tq.unlockModify()

	// Have the modify lock.

	peeked := tq.messageHeap.peekMessage()

	// hadTimer := len(tq.messages) > 0
	// var timerAt time.Time
	// if hadTimer {
	// 	timerAt = tq.messages[0]
	// }

	// TODO wrap id already in use error
	result, err := tq.messageHeap.pushMessage(id.String(), at, priority, value)
	if err != nil {
		return Message[T]{}, err
	}

	// earliest := tq.messages[0]
	// if !hadTimer || (hadTimer && earliest.Before(timerAt)) {
	// 	go func() {
	// 		tq.preempt <- preemptData{hadTimer: hadTimer, earliest: earliest}
	// 	}()
	// }

	earliest := tq.messageHeap.peekMessage()

	hadTimer := peeked != nil
	if !hadTimer || (hadTimer && earliest.At.Before(peeked.At)) {
		go func() {
			select {
			case tq.preempt <- preemptData{hadTimer: hadTimer, earliest: earliest.At}:
			case <-tq.preemptContext.Done():
			}
		}()
	}

	return *result, nil
}

type preemptData struct {
	hadTimer bool
	earliest time.Time
}

func (tq *TimeQueue[T]) Unload(ctx context.Context) ([]*Message[T], error) {
	if err := tq.lockModify(ctx); err != nil {
		return nil, err
	}
	defer tq.unlockModify()

	defer tq.setPreemptCancel()

	tq.preemptCancel()

	return tq.messageHeap.unload(), nil
}

func (tq *TimeQueue[T]) Remove(ctx context.Context, id string) (*Message[T], error) {
	if err := tq.lockModify(ctx); err != nil {
		return nil, err
	}
	defer tq.unlockModify()

	// TODO wrap error.
	return tq.messageHeap.removeMessage(id)
}

func (tq *TimeQueue[_]) lockModify(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tq.modify:
		return nil
	}
}

func (tq *TimeQueue[_]) unlockModify() {
	tq.modify <- struct{}{}
}

func (tq *TimeQueue[_]) setPreemptCancel() {
	tq.preemptContext, tq.preemptCancel = context.WithCancel(context.Background())
}
