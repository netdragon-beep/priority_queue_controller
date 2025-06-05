import os
import time
import redis
from kubernetes import client, config, k8s
from apscheduler.schedulers.background import BackgroundScheduler
from my_project import reconciler, queue, scheduler

def main():
    # 1. 获取 kubeconfig
    config.load_kube_config()

    # 2. 新建一个 Scheme
    v1 = client.CoreV1Api()

    # 3. 创建 Manager（模拟 ControllerManager）
    mgr = k8s.Manager()

    # 4. 初始化 Redis 队列
    redis_addr = os.getenv("REDIS_ADDR", "redis:6379")
    q = queue.RedisPriorityQueue(redis_addr, "taskqueue", 2 * 60)

    # 5. 注册 Reconciler
    reconciler.register_with_manager(mgr, q)

    # 6. 启动 anti-starvation 提升和 Dispatcher
    scheduler.start_promotion(q)
    scheduler.start_dispatcher(mgr.client, q)

    # 7. 启动控制器
    print("priority-queue controller started")
    mgr.start()

if __name__ == "__main__":
    main()
