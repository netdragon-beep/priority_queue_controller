# Kubernetes Priority Queue Controller

This project implements a Kubernetes controller that manages task scheduling using a priority queue. It watches for TaskRequest custom resources and processes them according to their priority.

## Project Structure

```
python/
├── priority_queue.py     # Priority queue implementation
├── controller.py         # Kubernetes controller implementation
├── scheduler.py          # Task scheduler implementation
├── reconciler.py         # Reconciliation logic
├── main.py               # Main entry point
├── config.py             # Configuration handling
├── config.yaml           # Sample configuration
└── requirements.txt      # Dependencies
```

## Features

- **Priority Queue**: Both in-memory and Redis-backed implementations
- **Kubernetes Integration**: Watches for TaskRequest CRDs and processes them
- **Dynamic Priority Adjustment**: Prevents task starvation by adjusting priorities over time
- **Configurable**: Easily configure through YAML or environment variables
- **Listener Interface**: Integration point for external services to submit tasks

## Installation

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Configure the application:
   - Edit `config.yaml` or set environment variables

3. Apply the TaskRequest CRD to your Kubernetes cluster:
   ```
   kubectl apply -f path/to/taskrequest-crd.yaml
   ```

## Usage

### Running the Controller

```python
python main.py
```

### Creating a TaskRequest

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

### Integration with Listener Group

The controller provides an interface for external services to submit tasks directly to the queue without going through Kubernetes. This can be used by the Listener Group to submit user requests.

Example:
```python
from controller import ListenerInterface

# Create a listener interface
listener = ListenerInterface(queue)

# Submit a task
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

## Configuration Options

### Kubernetes
- `use_incluster`: Whether to use in-cluster configuration
- `kubeconfig_path`: Path to kubeconfig file
- `namespace`: Namespace to watch

### Queue
- `type`: Queue type ("memory" or "redis")
- `redis_host`: Redis host
- `redis_port`: Redis port
- `queue_name`: Name of the queue
- `ttl`: Time-to-live for queue items
- `max_size`: Maximum queue size
- `default_priority`: Default priority for tasks

### Scheduler
- `poll_interval_seconds`: Interval between polling the queue
- `retry_limit`: Maximum retry attempts
- `retry_backoff_seconds`: Delay between retries

### Promotion
- `enabled`: Enable priority promotion
- `interval_seconds`: Interval between promotions
- `age_factor`: Factor for priority adjustment
- `max_boost`: Maximum priority boost

### Listener
- `enabled`: Enable listener interface
- `mode`: Listener mode ("http" or "grpc")
- `port`: Listener port

## Go to Python Conversion Notes

This project was converted from a Go implementation to Python. The following mappings were used:

| Go Feature | Python Equivalent |
|------------|-------------------|
| goroutines | asyncio tasks     |
| channels   | asyncio queues    |
| sync.Mutex | asyncio.Lock      |
| client-go  | kubernetes-client |
| container/heap | heapq module |

## Testing

Run the tests with:
```
pytest
```

## Example Test Case

```python
import pytest
import asyncio
from priority_queue import create_queue

@pytest.mark.asyncio
async def test_priority_queue():
    # Create an in-memory queue
    queue = create_queue("memory")
    
    # Add tasks with different priorities
    await queue.enqueue(priority=100, task_id="task1", payload={"data": "task1"})
    await queue.enqueue(priority=50, task_id="task2", payload={"data": "task2"})
    await queue.enqueue(priority=150, task_id="task3", payload={"data": "task3"})
    
    # Verify tasks are dequeued in priority order
    result1 = await queue.dequeue()
    assert result1[1] == "task2"  # Lowest priority value (highest priority) first
    
    result2 = await queue.dequeue()
    assert result2[1] == "task1"
    
    result3 = await queue.dequeue()
    assert result3[1] == "task3"
    
    # Verify queue is empty
    assert await queue.dequeue() is None
