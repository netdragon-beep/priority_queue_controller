package controller

import (
	"context"

	v1alpha1 "priority-queue-controller/pkg/api/v1alpha1" // replace with your module path
	"priority-queue-controller/pkg/queue"                 // replace with your module path

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TaskRequestReconciler watches TaskRequest CRs and enqueues newly‑created ones
// into the RedisPriorityQueue.

type TaskRequestReconciler struct {
	client.Client
	Q *queue.RedisPriorityQueue
}

func (r *TaskRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var tr v1alpha1.TaskRequest
	if err := r.Get(ctx, req.NamespacedName, &tr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if tr.Annotations == nil {
		tr.Annotations = map[string]string{}
	}
	if tr.Annotations["scheduler.rcme.ai/enqueued"] == "true" {
		return ctrl.Result{}, nil // already processed
	}

	if err := r.Q.Enqueue(ctx, &tr); err != nil {
		return ctrl.Result{}, err
	}
	tr.Annotations["scheduler.rcme.ai/enqueued"] = "true"
	return ctrl.Result{}, r.Update(ctx, &tr)
}

func (r *TaskRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.TaskRequest{}).
		Complete(r)
}
