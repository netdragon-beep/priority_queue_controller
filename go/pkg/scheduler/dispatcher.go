package scheduler

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "priority-queue-controller/pkg/api/v1alpha1" // replace with your module path
	"priority-queue-controller/pkg/queue"                 // replace with your module path
)

// priority‑class mapping – adjust to your PriorityClass names.
var pcMap = map[int]string{
	0: "pq-high",
	1: "pq-high",
	2: "pq-default",
	3: "pq-low",
	4: "pq-low",
	5: "pq-low",
}

// StartDispatcher continuously dequeues TaskRequests and spawns Jobs.
func StartDispatcher(ctx context.Context, cli client.Client, q *queue.RedisPriorityQueue) {
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
		if nn.Name == "" { // queue empty
			time.Sleep(time.Second)
			continue
		}

		var tr v1alpha1.TaskRequest
		if err := cli.Get(ctx, nn, &tr); err != nil {
			if errors.IsNotFound(err) {
				continue // stale entry
			}
			time.Sleep(time.Second)
			continue
		}

		// Build Job spec (simplified)
		img := tr.Spec.Payload["image"]
		if img == "" {
			img = "alpine:3"
		}
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: fmt.Sprintf("%s-job-", tr.Name),
				Namespace:    tr.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(&tr, schema.GroupVersionKind{Group: v1alpha1.Group, Version: v1alpha1.Version, Kind: "TaskRequest"}),
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
			fmt.Printf("failed to create Job for %s: %v\n", tr.Name, err)
		}
	}
}
