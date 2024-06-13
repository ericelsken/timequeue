package timequeue

import (
	"context"
	"fmt"
	"log"
	"time"
)

type Message[T any] struct {
	ID    string
	At    time.Time
	Value T
}

type TimeQueue struct {
	timer *time.Timer

	dequeue chan struct{}

	preempt chan preemptData

	modify chan struct{}

	messages []time.Time
}

func New() *TimeQueue {
	tq := &TimeQueue{
		timer:   time.NewTimer(0),
		dequeue: make(chan struct{}, 1),
		modify:  make(chan struct{}, 1),
		preempt: make(chan preemptData, 1),
	}
	<-tq.timer.C
	tq.dequeue <- struct{}{}
	tq.modify <- struct{}{}
	return tq
}

func (tq *TimeQueue) Dequeue(ctx context.Context) (time.Time, error) {
	select {
	case <-ctx.Done():
		return time.Time{}, ctx.Err()
	case <-tq.dequeue:
		defer func() {
			tq.dequeue <- struct{}{}
		}()
	}

	// Have the dequeue lock. There is just one goroutine here.

	var now time.Time
	var haveTimerValue bool
	for !haveTimerValue {
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()

		case preempted := <-tq.preempt:
			log.Println("preempted", preempted)
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

	log.Println("dequeue'd now", now)

	// Have the dequeue lock, an expired timer, and can pull a value off of messages.

	select {
	case <-ctx.Done():
		return time.Time{}, ctx.Err()
	case <-tq.modify:
		defer func() {
			tq.modify <- struct{}{}
		}()
	}

	// Have the dequeue lock, expired timer, and modify lock.

	var result time.Time
	if len(tq.messages) == 0 {
		return time.Time{}, fmt.Errorf("empty")
	}
	result = tq.messages[0]
	tq.messages = tq.messages[1:]

	// TODO reset the timer to the nearest time.
	if len(tq.messages) > 0 {
		earliest := tq.messages[0]
		// tq.resetTimer(earliest)
		tq.timer.Reset(time.Until(earliest))
	}

	return result, nil
}

type preemptData struct {
	hadTimer bool
	earliest time.Time
}

// func (tq *TimeQueue) resetTimer(at time.Time) {
// 	// if !tq.timer.Stop() {
// 	// 	log.Println("waiting on resetTimer()")
// 	// 	<-tq.timer.C
// 	// }
// 	tq.timer.Reset(time.Until(at))
// }

func (tq *TimeQueue) Enqueue(ctx context.Context, value time.Time) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tq.modify:
		defer func() {
			tq.modify <- struct{}{}
		}()
	}

	// Have the modify lock.

	hadTimer := len(tq.messages) > 0
	var timerAt time.Time
	if hadTimer {
		timerAt = tq.messages[0]
	}

	tq.messages = append(tq.messages, value)

	earliest := tq.messages[0]

	log.Println("there are new messages", hadTimer, earliest.Before(timerAt))

	if !hadTimer || (hadTimer && earliest.Before(timerAt)) {

		go func() {
			tq.preempt <- preemptData{hadTimer: hadTimer, earliest: earliest}
		}()
	}

	return nil
}
