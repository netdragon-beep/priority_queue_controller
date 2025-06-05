"""
Tests for the priority queue implementation.

This module contains tests for both the in-memory and Redis-backed
priority queue implementations.
"""

import pytest
import asyncio
import time
from typing import Dict, Any

from priority_queue import create_queue, PriorityQueue, RedisPriorityQueue

# Skip Redis tests if Redis is not available
redis_available = True
try:
    import redis
    r = redis.Redis(host='localhost', port=6379, db=0)
    r.ping()
except:
    redis_available = False

@pytest.mark.asyncio
async def test_memory_queue_basic_operations():
    """Test basic operations on the in-memory priority queue."""
    # Create queue
    queue = create_queue("memory")
    
    # Test enqueue and dequeue
    await queue.enqueue(priority=100, task_id="task1", payload={"data": "task1"})
    result = await queue.dequeue()
    
    assert result is not None
    assert result[0] == 100
    assert result[1] == "task1"
    assert result[2] == {"data": "task1"}
    
    # Queue should be empty now
    assert await queue.dequeue() is None
    
@pytest.mark.asyncio
async def test_memory_queue_priority_order():
    """Test that tasks are dequeued in priority order."""
    # Create queue
    queue = create_queue("memory")
    
    # Add tasks with different priorities
    await queue.enqueue(priority=100, task_id="task1", payload={"data": "task1"})
    await queue.enqueue(priority=50, task_id="task2", payload={"data": "task2"})  # Higher priority
    await queue.enqueue(priority=150, task_id="task3", payload={"data": "task3"})  # Lower priority
    
    # Verify tasks are dequeued in priority order
    result1 = await queue.dequeue()
    assert result1[1] == "task2"  # Highest priority (lowest value) first
    
    result2 = await queue.dequeue()
    assert result2[1] == "task1"  # Medium priority second
    
    result3 = await queue.dequeue()
    assert result3[1] == "task3"  # Lowest priority last
    
    # Queue should be empty now
    assert await queue.dequeue() is None

@pytest.mark.asyncio
async def test_memory_queue_contains_and_remove():
    """Test contains and remove operations."""
    # Create queue
    queue = create_queue("memory")
    
    # Add task
    await queue.enqueue(priority=100, task_id="task1", payload={"data": "task1"})
    
    # Check if task is in queue
    assert await queue.contains("task1") is True
    assert await queue.contains("task2") is False
    
    # Remove task
    assert await queue.remove("task1") is True
    assert await queue.contains("task1") is False
    
    # Try to remove non-existent task
    assert await queue.remove("task2") is False

@pytest.mark.asyncio
async def test_memory_queue_adjust_priority():
    """Test priority adjustment."""
    # Create queue
    queue = create_queue("memory")
    
    # Add task
    await queue.enqueue(priority=100, task_id="task1", payload={"data": "task1"})
    
    # Adjust priority
    assert await queue.adjust_priority("task1", 50) is True
    
    # Verify new priority
    peek_result = await queue.peek()
    assert peek_result[0] == 50  # New priority
    
    # Try to adjust priority of non-existent task
    assert await queue.adjust_priority("task2", 50) is False

@pytest.mark.asyncio
async def test_memory_queue_dynamic_priority_adjustment():
    """Test dynamic priority adjustment."""
    # Create queue
    queue = create_queue("memory")
    
    # Add tasks with timestamps in the past
    for i in range(5):
        item = queue._QueueItem(
            priority=100,
            timestamp=time.time() - (i + 1) * 10,  # 10, 20, 30, 40, 50 seconds in the past
            task_id=f"task{i}",
            payload={"data": f"task{i}"}
        )
        queue._queue.append(item)
        queue._item_lookup[f"task{i}"] = item
    
    # Run dynamic priority adjustment
    adjusted_count = await queue.dynamic_priority_adjustment(age_factor=1.0, max_boost=20)
    
    # Should have adjusted all 5 tasks
    assert adjusted_count == 5
    
    # Tasks should have had their priorities boosted based on age
    for item in queue._queue:
        assert item.priority < 100  # Priority should be lower (higher importance)

@pytest.mark.asyncio
@pytest.mark.skipif(not redis_available, reason="Redis not available")
async def test_redis_queue_basic_operations():
    """Test basic operations on the Redis-backed priority queue."""
    # Create queue with a unique name for testing
    queue_name = f"test_queue_{int(time.time())}"
    queue = create_queue("redis", redis_host="localhost", queue_name=queue_name, ttl=10)
    
    try:
        # Test enqueue and dequeue
        await queue.enqueue(priority=100, task_id="task1", payload={"data": "task1"})
        result = await queue.dequeue()
        
        assert result is not None
        assert result[0] == 100
        assert result[1] == "task1"
        assert result[2] == {"data": "task1"}
        
        # Queue should be empty now
        assert await queue.dequeue() is None
    finally:
        # Clean up Redis keys
        redis_client = redis.Redis(host='localhost', port=6379, db=0)
        redis_client.delete(queue_name)
        redis_client.delete(f"{queue_name}_timestamps")

@pytest.mark.asyncio
@pytest.mark.skipif(not redis_available, reason="Redis not available")
async def test_redis_queue_priority_order():
    """Test that tasks are dequeued in priority order from Redis queue."""
    # Create queue with a unique name for testing
    queue_name = f"test_queue_{int(time.time())}"
    queue = create_queue("redis", redis_host="localhost", queue_name=queue_name, ttl=10)
    
    try:
        # Add tasks with different priorities
        await queue.enqueue(priority=100, task_id="task1", payload={"data": "task1"})
        await queue.enqueue(priority=50, task_id="task2", payload={"data": "task2"})  # Higher priority
        await queue.enqueue(priority=150, task_id="task3", payload={"data": "task3"})  # Lower priority
        
        # Verify tasks are dequeued in priority order
        result1 = await queue.dequeue()
        assert result1[1] == "task2"  # Highest priority (lowest value) first
        
        result2 = await queue.dequeue()
        assert result2[1] == "task1"  # Medium priority second
        
        result3 = await queue.dequeue()
        assert result3[1] == "task3"  # Lowest priority last
        
        # Queue should be empty now
        assert await queue.dequeue() is None
    finally:
        # Clean up Redis keys
        redis_client = redis.Redis(host='localhost', port=6379, db=0)
        redis_client.delete(queue_name)
        redis_client.delete(f"{queue_name}_timestamps")
