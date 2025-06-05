package queue

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// 验证定期提升老任务的优先级
func TestPromotion(t *testing.T) {
	promoteDur := 50 * time.Millisecond
	q, done := setupQueue(t, promoteDur)
	defer done()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q.StartPromotion(ctx) // 后台提升 goroutine

	low := newTR("default", "low", 5)
	if err := q.Enqueue(ctx, low); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	orig, _ := q.client.ZScore(ctx, q.key, keyFor(low)).Result()
	time.Sleep(2 * promoteDur) // 至少提升一次
	after, _ := q.client.ZScore(ctx, q.key, keyFor(low)).Result()
	if after >= orig {
		t.Fatalf("score not decreased: before=%f after=%f", orig, after)
	}

	// 再插入一个同优先级的新任务，应该先出队 old->low
	newer := newTR("default", "new", 5)
	_ = q.Enqueue(ctx, newer)

	nn, _ := q.Dequeue(ctx)
	if nn != (types.NamespacedName{Namespace: "default", Name: "low"}) {
		t.Fatalf("expect promoted 'low' first, got %v", nn.Name)
	}
}
