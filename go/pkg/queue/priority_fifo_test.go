package queue

import (
	"context"
	"testing"
	"time"

	v1alpha1 "priority-queue-controller/pkg/api/v1alpha1"

	"k8s.io/apimachinery/pkg/types"
)

// 验证优先级 + 同优先级下 FIFO
func TestPriorityAndFIFO(t *testing.T) {
	q, done := setupQueue(t, time.Minute)
	defer done()
	ctx := context.Background()

	trA := newTR("default", "a", 3)
	time.Sleep(10 * time.Millisecond)
	trB := newTR("default", "b", 1)
	time.Sleep(10 * time.Millisecond)
	trC := newTR("default", "c", 3)

	// 注意这里要写 v1alpha1.TaskRequest
	for _, tr := range []*v1alpha1.TaskRequest{trA, trB, trC} {
		if err := q.Enqueue(ctx, tr); err != nil {
			t.Fatalf("enqueue %s: %v", tr.Name, err)
		}
	}

	check := func(want types.NamespacedName) {
		nn, _ := q.Dequeue(ctx)
		if nn != want {
			t.Fatalf("want %v, got %v", want, nn)
		}
	}

	check(types.NamespacedName{Namespace: "default", Name: "b"}) // prio-1
	check(types.NamespacedName{Namespace: "default", Name: "a"}) // FIFO
	check(types.NamespacedName{Namespace: "default", Name: "c"}) // FIFO
}
