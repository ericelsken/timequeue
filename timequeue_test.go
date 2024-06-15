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
	tq := New[int]()

	if _, err := tq.EnqueueValue(background, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := tq.EnqueueValue(background, 3); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(background, time.Second)
	defer cancel()
	message, _, err := tq.Dequeue(ctx)
	if err != nil {
		t.Log(err)
	}
	t.Log("got value", message.Value)
}
