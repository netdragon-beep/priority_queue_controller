package queue

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// 动态插队 / 抢占场景
func TestDynamicEnqueuePreemption(t *testing.T) {
	q, done := setupQueue(t, time.Minute)
	defer done()
	ctx := context.Background()

	low := newTR("default", "low", 5)
	high := newTR("default", "high", 1)

	_ = q.Enqueue(ctx, low)
	_ = q.Enqueue(ctx, high)

	nn, _ := q.Dequeue(ctx)
	if nn != (types.NamespacedName{Namespace: "default", Name: "high"}) {
		t.Fatalf("preemption fail: want high, got %v", nn.Name)
	}

	nn, _ = q.Dequeue(ctx)
	if nn != (types.NamespacedName{Namespace: "default", Name: "low"}) {
		t.Fatalf("remaining order fail: want low, got %v", nn.Name)
	}
}
