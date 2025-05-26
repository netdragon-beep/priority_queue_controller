// priority_queue_test.go
package main

import (
	"context"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// 帮助函数：快速生成 TaskRequest
func newTR(ns, name string, prio int) *TaskRequest {
	return &TaskRequest{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
		},
		Spec: TaskRequestSpec{
			Priority: prio,
			Payload:  map[string]string{},
		},
	}
}

func setupQueue(t *testing.T, promote time.Duration) (*RedisPriorityQueue, func()) {
	t.Helper()

	// 启动一个内存里的 Redis
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	cleanup := func() { mr.Close() }

	// 构造队列
	q := NewRedisPriorityQueue(mr.Addr(), "testqueue", promote)
	q.client = redis.NewClient(&redis.Options{Addr: mr.Addr()}) // 覆盖内部 client

	return q, cleanup
}

func TestPriorityAndFIFO(t *testing.T) {
	q, done := setupQueue(t, time.Minute)
	defer done()
	ctx := context.Background()

	// 依次入队：低优先级 3，再高优先级 1，再低优先级 3
	trA := newTR("default", "a", 3)
	time.Sleep(10 * time.Millisecond) // 保证时间戳不同
	trB := newTR("default", "b", 1)
	time.Sleep(10 * time.Millisecond)
	trC := newTR("default", "c", 3)

	if err := q.Enqueue(ctx, trA); err != nil {
		t.Fatalf("enqueue A: %v", err)
	}
	if err := q.Enqueue(ctx, trB); err != nil {
		t.Fatalf("enqueue B: %v", err)
	}
	if err := q.Enqueue(ctx, trC); err != nil {
		t.Fatalf("enqueue C: %v", err)
	}

	// 第一次出队应该是优先级 1 的 B
	nn, _ := q.Dequeue(ctx)
	want := types.NamespacedName{Namespace: "default", Name: "b"}
	if nn != want {
		t.Fatalf("want %v, got %v", want, nn)
	}

	// 剩下同优先级 3，应按 FIFO 出队 A 再 C
	nn, _ = q.Dequeue(ctx)
	want = types.NamespacedName{Namespace: "default", Name: "a"}
	if nn != want {
		t.Fatalf("FIFO fail: want %v, got %v", want, nn)
	}

	nn, _ = q.Dequeue(ctx)
	want = types.NamespacedName{Namespace: "default", Name: "c"}
	if nn != want {
		t.Fatalf("FIFO fail: want %v, got %v", want, nn)
	}
}

func TestPromotion(t *testing.T) {
	// 将 promoteDur 设为 50 ms，加快测试
	promoteDur := 50 * time.Millisecond
	q, done := setupQueue(t, promoteDur)
	defer done()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动 promotion goroutine
	q.StartPromotion(ctx)

	low := newTR("default", "low", 5) // 优先级最低
	if err := q.Enqueue(ctx, low); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// 记录原 score
	orig, _ := q.client.ZScore(ctx, q.key, keyFor(low)).Result()

	// 等待两倍 promoteDur，确保至少一次提升
	time.Sleep(2 * promoteDur)

	after, _ := q.client.ZScore(ctx, q.key, keyFor(low)).Result()
	if after >= orig {
		t.Fatalf("score not decreased: before=%f after=%f", orig, after)
	}

	// 继续插入一个同优先级但刚刚入队的任务，
	// 检验老任务因提升而先出。
	newer := newTR("default", "new", 5)
	if err := q.Enqueue(ctx, newer); err != nil {
		t.Fatalf("enqueue newer: %v", err)
	}

	nn, _ := q.Dequeue(ctx)
	if nn.Name != "low" {
		t.Fatalf("expect promoted task 'low' first, got %v", nn.Name)
	}
}

func TestDynamicEnqueuePreemption(t *testing.T) {
	q, done := setupQueue(t, time.Minute)
	defer done()
	ctx := context.Background()

	// 先入队一个低优先级任务
	low := newTR("default", "low", 5)
	if err := q.Enqueue(ctx, low); err != nil {
		t.Fatalf("enqueue low: %v", err)
	}

	// 再入队一个高优先级任务（优先级数字更小）
	high := newTR("default", "high", 1)
	if err := q.Enqueue(ctx, high); err != nil {
		t.Fatalf("enqueue high: %v", err)
	}

	// 第一次出队，应该是高优先级的 high
	nn, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("dequeue first: %v", err)
	}
	want := types.NamespacedName{Namespace: "default", Name: "high"}
	if nn != want {
		t.Fatalf("preemption fail: want %v, got %v", want, nn)
	}

	// 第二次出队，再返回低优先级的 low
	nn, err = q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("dequeue second: %v", err)
	}
	want = types.NamespacedName{Namespace: "default", Name: "low"}
	if nn != want {
		t.Fatalf("remaining order fail: want %v, got %v", want, nn)
	}
}
