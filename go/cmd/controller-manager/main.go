package main

import (
	"fmt"
	"os"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"

	v1alpha1 "priority-queue-controller/pkg/api/v1alpha1"
	"priority-queue-controller/pkg/controller"
	"priority-queue-controller/pkg/queue"
	"priority-queue-controller/pkg/scheduler"
)

func main() {
	// 1. 获取 kubeconfig
	cfg := ctrl.GetConfigOrDie()

	// 2. 新建一个 Scheme（不要用 ctrl.Scheme）
	scheme := runtime.NewScheme()

	// 3. 把 core/v1、batch/v1 以及自定义 CRD 注册进去
	_ = corev1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = v1alpha1.AddToScheme(scheme)

	// 4. 创建 Manager，把 scheme 传进去
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
	})
	if err != nil {
		panic(err)
	}

	// 5. 初始化 Redis 队列
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}
	q := queue.NewRedisPriorityQueue(redisAddr, "taskqueue", 2*time.Minute)

	// 6. 注册 Reconciler
	reconciler := &controller.TaskRequestReconciler{
		Client: mgr.GetClient(),
		Q:      q,
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		panic(err)
	}

	// 7. 启动 anti-starvation 提升和 Dispatcher
	ctx := ctrl.SetupSignalHandler()
	q.StartPromotion(ctx)
	go scheduler.StartDispatcher(ctx, mgr.GetClient(), q)

	fmt.Println("priority-queue controller started")
	if err := mgr.Start(ctx); err != nil {
		panic(err)
	}
}
