以下是该项目 README 的中文翻译，保留了原有的结构与格式。

---

## Kubernetes 优先队列控制器

该项目实现了一个 Kubernetes 控制器，用于通过优先级队列管理任务调度。它监听 `TaskRequest` 自定义资源，并根据优先级对它们进行处理。

### 项目结构

```
python/
├── priority_queue.py     # 优先队列实现
├── controller.py         # Kubernetes 控制器实现
├── scheduler.py          # 任务调度器实现
├── reconciler.py         # 对账逻辑
├── main.py               # 主入口
├── config.py             # 配置处理
├── config.yaml           # 示例配置
└── requirements.txt      # 依赖项
```

### 特性

- **优先队列**：支持内存和 Redis 两种实现
- **Kubernetes 集成**：监听 `TaskRequest` CRD 并进行处理
- **动态优先级调整**：通过随时间调整优先级来防止任务饥饿
- **可配置**：可通过 YAML 或环境变量进行配置
- **监听器接口**：为外部服务提供直接向队列提交任务的集成点

### 安装

安装依赖项：

```bash
pip install -r requirements.txt
```

配置应用：

- 编辑 `config.yaml` 或设置相应的环境变量

将 `TaskRequest` CRD 应用到 Kubernetes 集群：

```bash
kubectl apply -f path/to/taskrequest-crd.yaml
```

### 使用

#### 运行控制器

```bash
python main.py
```

#### 创建一个 TaskRequest

```yaml
apiVersion: scheduler.rcme.ai/v1alpha1
kind: TaskRequest
metadata:
  name: sample-task
spec:
  priority: 50
  payload:
    model: "gpt-4"
    prompt: "Hello, world!"
    parameters:
      temperature: 0.7
      max_tokens: 1000
```

#### 与监听器组集成

该控制器提供了一个接口，允许外部服务直接向队列提交任务，而无需经过 Kubernetes。监听器组可以通过此方式提交用户请求。

示例：

```python
from controller import ListenerInterface

# 创建一个监听器接口
listener = ListenerInterface(queue)

# 提交一个任务
await listener.submit_task(
    task_id="user-request-123",
    priority=50,
    payload={
        "model": "gpt-4",
        "prompt": "Hello, world!",
        "parameters": {
            "temperature": 0.7,
            "max_tokens": 1000
        }
    }
)
```

### 配置选项

#### Kubernetes

- `use_incluster`：是否使用集群内部配置
- `kubeconfig_path`：kubeconfig 文件路径
- `namespace`：监听的命名空间

#### 队列

- `type`：队列类型（“memory” 或 “redis”）
- `redis_host`：Redis 主机
- `redis_port`：Redis 端口
- `queue_name`：队列名称
- `ttl`：队列项目的生存时间
- `max_size`：队列最大容量
- `default_priority`：任务的默认优先级

#### 调度器

- `poll_interval_seconds`：轮询队列的间隔（秒）
- `retry_limit`：最大重试次数
- `retry_backoff_seconds`：重试间隔（秒）

#### 优先提升

- `enabled`：是否启用优先级提升
- `interval_seconds`：执行提升的间隔（秒）
- `age_factor`：优先级调整系数
- `max_boost`：最大优先级提升值

#### 监听器

- `enabled`：是否启用监听器接口
- `mode`：监听器模式（“http” 或 “grpc”）
- `port`：监听器端口

### Go 到 Python 的对应说明

该项目从 Go 实现转换到 Python 时，使用了以下对应关系：

| Go 特性           | Python 等价      |
| ----------------- | ---------------- |
| goroutines        | asyncio 任务     |
| channels          | asyncio 队列     |
| sync.Mutex        | asyncio.Lock     |
| client-go         | kubernetes-client|
| container/heap    | heapq 模块       |

### 测试

使用以下命令运行测试：

```bash
pytest
```

#### 示例测试用例

```python
import pytest
import asyncio
from priority_queue import create_queue

@pytest.mark.asyncio
async def test_priority_queue():
    # 创建一个内存队列
    queue = create_queue("memory")
    
    # 添加不同优先级的任务
    await queue.enqueue(priority=100, task_id="task1", payload={"data": "task1"})
    await queue.enqueue(priority=50, task_id="task2", payload={"data": "task2"})
    await queue.enqueue(priority=150, task_id="task3", payload={"data": "task3"})
    
    # 验证任务按优先级顺序出队
    result1 = await queue.dequeue()
    assert result1[1] == "task2"  # 优先级值最小（优先级最高）先出
    
    result2 = await queue.dequeue()
    assert result2[1] == "task1"
    
    result3 = await queue.dequeue()
    assert result3[1] == "task3"
    
    # 验证队列已空
    assert await queue.dequeue() is None
```

--- 

以上即为 README 的中文翻译版本，涵盖了项目介绍、结构、特性、安装步骤、使用说明、配置选项、Go 与 Python 对应说明，以及测试示例。若有进一步需求，可根据此模版修改或补充。
