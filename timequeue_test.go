package timequeue

import (
	"context"
	"testing"
	"time"
)

var (
	background = context.Background()
)

func TestTimeQueue(t *testing.T) {
	tq := New()
	t.Log(tq)

	now := time.Now()

	if err := tq.Enqueue(background, now); err != nil {
		t.Fatal(err)
	}
	if err := tq.Enqueue(background, now.Add(-1*time.Second)); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(background, time.Second)
	defer cancel()
	value, err := tq.Dequeue(ctx)
	if err != nil {
		t.Log(value, err)
	}

	t.Log(tq.messages)
}
