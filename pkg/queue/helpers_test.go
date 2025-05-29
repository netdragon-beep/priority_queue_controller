package queue

import (
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "priority-queue-controller/pkg/api/v1alpha1" // ← 新增
)

// newTR 创建一个简单的 TaskRequest。
func newTR(ns, name string, prio int) *v1alpha1.TaskRequest { // ← 返回类型加前缀
	return &v1alpha1.TaskRequest{ // ← 结构体加前缀
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
		},
		Spec: v1alpha1.TaskRequestSpec{ // ← 字段类型加前缀
			Priority: prio,
			Payload:  map[string]string{},
		},
	}
}

func setupQueue(t *testing.T, promote time.Duration) (*RedisPriorityQueue, func()) {
	t.Helper()

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	cleanup := func() { mr.Close() }

	q := NewRedisPriorityQueue(mr.Addr(), "testqueue", promote)
	q.client = redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return q, cleanup
}
