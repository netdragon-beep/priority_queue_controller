package queue

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	v1alpha1 "priority-queue-controller/pkg/api/v1alpha1" // replace with your module path

	"github.com/go-redis/redis/v8"
	"k8s.io/apimachinery/pkg/types"
)

// RedisPriorityQueue is a priority queue backed by Redis sorted-set.
// It supports FIFO ordering within the same priority and an anti-starvation
// promotion mechanism that gradually lifts the priority of long‑waiting tasks.

type RedisPriorityQueue struct {
	key        string
	client     *redis.Client
	promoteDur time.Duration
}

// NewRedisPriorityQueue returns a new RedisPriorityQueue instance.
func NewRedisPriorityQueue(addr, key string, promoteDur time.Duration) *RedisPriorityQueue {
	return &RedisPriorityQueue{
		key:        key,
		client:     redis.NewClient(&redis.Options{Addr: addr}),
		promoteDur: promoteDur,
	}
}

// score = base(priority)*1e12 + unixNano – keeps FIFO order within a priority bucket.
func scoreFor(p int) float64 { return float64(p) * 1e12 }

// Enqueue adds the TaskRequest to the queue.
func (q *RedisPriorityQueue) Enqueue(ctx context.Context, tr *v1alpha1.TaskRequest) error {
	s := scoreFor(tr.Spec.Priority) + float64(time.Now().UnixNano())
	return q.client.ZAdd(ctx, q.key, &redis.Z{Member: keyFor(tr), Score: s}).Err()
}

// Dequeue pops the highest‑priority (lowest score) TaskRequest key from the queue.
func (q *RedisPriorityQueue) Dequeue(ctx context.Context) (types.NamespacedName, error) {
	res, err := q.client.ZPopMin(ctx, q.key, 1).Result()
	if err != nil {
		return types.NamespacedName{}, err
	}
	if len(res) == 0 {
		return types.NamespacedName{}, nil // empty queue
	}
	return parseKey(res[0].Member.(string)), nil
}

// StartPromotion periodically promotes long‑waiting tasks by decreasing their score.
func (q *RedisPriorityQueue) StartPromotion(ctx context.Context) {
	ticker := time.NewTicker(q.promoteDur)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := float64(time.Now().Add(-q.promoteDur).UnixNano())
				// candidate score upper‑bound for priority 5 tasks (lowest priority)
				upper := scoreFor(5) + now

				cands, err := q.client.ZRangeByScore(ctx, q.key, &redis.ZRangeBy{
					Min:   "-inf",
					Max:   strconv.FormatFloat(upper, 'f', -1, 64),
					Count: 100,
				}).Result()
				if err != nil {
					continue
				}
				for _, m := range cands {
					// lift priority by reducing score by 1e12 (one priority class)
					q.client.ZIncrBy(ctx, q.key, -1e12, m)
				}
			}
		}
	}()
}

// helper: namespace/name <-> redis member conversion
func keyFor(tr *v1alpha1.TaskRequest) string { return fmt.Sprintf("%s/%s", tr.Namespace, tr.Name) }

func parseKey(k string) types.NamespacedName {
	ss := strings.SplitN(k, "/", 2)
	if len(ss) != 2 {
		return types.NamespacedName{}
	}
	return types.NamespacedName{Namespace: ss[0], Name: ss[1]}
}
