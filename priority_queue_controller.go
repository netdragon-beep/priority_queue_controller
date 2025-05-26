// priority_queue_controller.go
// 一个示例性的单一二进制实现，演示了前面描述的 TaskRequest CRD 控制器，
// 队列的入队/出队优先级，防止饥饿的提升机制，以及任务生成器。
// 生产环境的部署应将不同的关注点（CRD 类型、API 注册、队列后端、调度器）分离成不同的包，并添加完整的错误处理、指标和领导者选举。

package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// -----------------------------------------------------------------------------
// 1. CRD Go 类型（匹配 YAML 定义）
// -----------------------------------------------------------------------------
type TaskRequestStatus struct {
	// Fill in once you need status fields
}

type TaskRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              TaskRequestSpec   `json:"spec,omitempty"`
	Status            TaskRequestStatus `json:"status,omitempty"`
}

type TaskRequestSpec struct {
	Priority int               `json:"priority"`
	Payload  map[string]string `json:"payload"` // 任意的键值对（例如：镜像，参数等）
}

// +kubebuilder:object:root=true
// TaskRequestList 用于缓存 informer。

type TaskRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TaskRequest `json:"items"`
}

// 将结构体注册到 Scheme 以便 controller-runtime 能进行序列化。
func addTaskRequestToScheme(s *runtime.Scheme) error {
	gvk := schema.GroupVersion{Group: "scheduler.rcme.ai", Version: "v1alpha1"}
	s.AddKnownTypes(gvk,
		&TaskRequest{},
		&TaskRequestList{},
	)
	metav1.AddToGroupVersion(s, gvk)
	return nil
}

// DeepCopyObject 实现 runtime.Object 接口
func (in *TaskRequest) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func (in *TaskRequestList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

// -----------------------------------------------------------------------------
// 2. 基于 Redis 的优先级队列，防止饥饿（任务提升机制）
// -----------------------------------------------------------------------------

type RedisPriorityQueue struct {
	key        string
	client     *redis.Client
	promoteDur time.Duration
}

func NewRedisPriorityQueue(addr, key string, promoteDur time.Duration) *RedisPriorityQueue {
	return &RedisPriorityQueue{
		key:        key,
		client:     redis.NewClient(&redis.Options{Addr: addr}),
		promoteDur: promoteDur,
	}
}

// score = 基础（优先级）* 1e12 + 时间戳纳秒  – 确保同一优先级下按 FIFO 排序。
func scoreFor(p int) float64 { return float64(p) * 1e12 }

func (q *RedisPriorityQueue) Enqueue(ctx context.Context, tr *TaskRequest) error {
	s := scoreFor(tr.Spec.Priority) + float64(time.Now().UnixNano())
	return q.client.ZAdd(ctx, q.key, &redis.Z{Member: keyFor(tr), Score: s}).Err()
}

func (q *RedisPriorityQueue) Dequeue(ctx context.Context) (types.NamespacedName, error) {
	res, err := q.client.ZPopMin(ctx, q.key, 1).Result()
	if err != nil {
		return types.NamespacedName{}, err
	}
	if len(res) == 0 {
		// 队列为空
		return types.NamespacedName{}, nil
	}
	return parseKey(res[0].Member.(string)), nil
}

// 防止饥饿的提升机制：对于等待时间超过 `promoteDur` 的任务，降低其分数（提高优先级）。
func (q *RedisPriorityQueue) StartPromotion(ctx context.Context) {
	ticker := time.NewTicker(q.promoteDur)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := float64(time.Now().Add(-q.promoteDur).UnixNano())

				// 查找那些等待时间超过 `promoteDur` 的任务。
				// 使用 Redis ZRANGEBYSCORE 查询符合条件的任务。
				cands, err := q.client.ZRangeByScore(ctx, q.key, &redis.ZRangeBy{
					Min:   "-inf",
					Max:   strconv.FormatFloat(scoreFor(5)+now, 'f', -1, 64),
					Count: 100,
				}).Result()
				if err != nil {
					continue
				}
				for _, m := range cands {
					// 提升任务优先级（通过减少分数 1e12）。
					q.client.ZIncrBy(ctx, q.key, -1e12, m)
				}
			}
		}
	}()
}

// 辅助方法：将 Redis 成员字符串 <-> namespace/name 映射。
func keyFor(tr *TaskRequest) string { return fmt.Sprintf("%s/%s", tr.Namespace, tr.Name) }

// func parseKey(k string) types.NamespacedName {
// 	parts := types.NamespacedName{}
// 	fmt.Sscanf(k, "%s/%s", &parts.Namespace, &parts.Name)
// 	return parts
// }

func parseKey(k string) types.NamespacedName {
	ss := strings.SplitN(k, "/", 2)
	if len(ss) != 2 {
		return types.NamespacedName{} // 格式不对就返回空
	}
	return types.NamespacedName{
		Namespace: ss[0],
		Name:      ss[1],
	}
}

// -----------------------------------------------------------------------------
// 3. 控制器：监听 TaskRequest，入队新的任务。
// -----------------------------------------------------------------------------

type TaskRequestReconciler struct {
	client.Client
	Q *RedisPriorityQueue
}

func (r *TaskRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var tr TaskRequest
	if err := r.Get(ctx, req.NamespacedName, &tr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if tr.Annotations == nil {
		tr.Annotations = map[string]string{}
	}
	if tr.Annotations["scheduler.rcme.ai/enqueued"] == "true" {
		return ctrl.Result{}, nil // 已经处理过
	}

	if err := r.Q.Enqueue(ctx, &tr); err != nil {
		return ctrl.Result{}, err
	}
	tr.Annotations["scheduler.rcme.ai/enqueued"] = "true"
	return ctrl.Result{}, r.Update(ctx, &tr)
}

// 优先级类映射，方便使用。
var pcMap = map[int]string{
	0: "pq-high",
	1: "pq-high",
	2: "pq-default",
	3: "pq-low",
	4: "pq-low",
	5: "pq-low",
}

// -----------------------------------------------------------------------------
// 4. 调度器：从队列中取任务并生成相应优先级的 Job（抢占机制）
// -----------------------------------------------------------------------------

func startDispatcher(ctx context.Context, mgr ctrl.Manager, q *RedisPriorityQueue) {
	cli := mgr.GetClient()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		nn, err := q.Dequeue(ctx)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		if nn.Name == "" { // 队列为空
			time.Sleep(time.Second)
			continue
		}

		var tr TaskRequest
		if err := cli.Get(ctx, nn, &tr); err != nil {
			if errors.IsNotFound(err) {
				continue // 任务已过期
			}
			time.Sleep(time.Second)
			continue
		}

		// 从 Payload 构建 Job 的规格（简化版）
		img := tr.Spec.Payload["image"]
		if img == "" {
			img = "alpine:3" // 默认镜像
		}
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: fmt.Sprintf("%s-job-", tr.Name),
				Namespace:    tr.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(&tr, schema.GroupVersionKind{Group: "scheduler.rcme.ai", Version: "v1alpha1", Kind: "TaskRequest"}),
				},
			},
			Spec: batchv1.JobSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:  "worker",
							Image: img,
						}},
						RestartPolicy:     corev1.RestartPolicyNever,
						PriorityClassName: pcMap[tr.Spec.Priority],
					},
				},
			},
		}
		if err := cli.Create(ctx, job); err != nil {
			// 为简化起见，未实现重试/回退机制
			fmt.Fprintf(os.Stderr, "为 %s 创建 Job 失败: %v\n", tr.Name, err)
		}
	}
}

// -----------------------------------------------------------------------------
// 5. 主函数 – 将所有组件连接在一起
// -----------------------------------------------------------------------------

func main() {
	cfg := ctrl.GetConfigOrDie()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = addTaskRequestToScheme(scheme)

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme})
	if err != nil {
		panic(err)
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}
	q := NewRedisPriorityQueue(redisAddr, "taskqueue", 2*time.Minute)

	reconciler := &TaskRequestReconciler{Client: mgr.GetClient(), Q: q}
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&TaskRequest{}).
		Complete(reconciler); err != nil {
		panic(err)
	}

	ctx := ctrl.SetupSignalHandler()
	q.StartPromotion(ctx)
	go startDispatcher(ctx, mgr, q)

	fmt.Println("优先级队列控制器已启动")
	if err := mgr.Start(ctx); err != nil {
		panic(err)
	}
}
