"""
Tests for the scheduler implementation.

This module contains tests for the scheduler that processes tasks from the priority queue.
"""

import pytest
import asyncio
import time
from unittest.mock import MagicMock, patch, AsyncMock

from priority_queue import create_queue
from scheduler import Scheduler, AsyncScheduler
from api.v1alpha1.taskrequest import create_task_request, task_request_to_dict


@pytest.mark.asyncio
async def test_scheduler_process_task_success():
    """Test successful task processing."""
    # Create a mock queue
    queue = create_queue("memory")
    
    # Create a mock dispatch callback
    dispatch_callback = AsyncMock()
    dispatch_callback.return_value = None
    
    # Create a scheduler
    scheduler = Scheduler(
        queue=queue,
        dispatch_callback=dispatch_callback,
        poll_interval=0.1
    )
    
    # Add a task to the queue
    task_request = create_task_request(
        name="test-task",
        namespace="default",
        priority=50,
        payload={"test": "data"}
    )
    task_dict = task_request_to_dict(task_request)
    
    await queue.enqueue(
        priority=50,
        task_id="test-task",
        payload=task_dict
    )
    
    # Process the task
    await scheduler.process_task("test-task", 50, task_dict)
    
    # Check that the dispatch callback was called
    dispatch_callback.assert_called_once_with("test-task", task_dict)


@pytest.mark.asyncio
async def test_scheduler_process_task_failure_retry():
    """Test task failure and retry."""
    # Create a mock queue
    queue = create_queue("memory")
    
    # Create a mock dispatch callback that raises an exception
    dispatch_callback = AsyncMock()
    dispatch_callback.side_effect = Exception("Test error")
    
    # Create a scheduler with short retry delay for testing
    scheduler = Scheduler(
        queue=queue,
        dispatch_callback=dispatch_callback,
        poll_interval=0.1,
        max_retries=2,
        retry_delay=0.1
    )
    
    # Add a task to the queue
    task_request = create_task_request(
        name="test-task",
        namespace="default",
        priority=50,
        payload={"test": "data"}
    )
    task_dict = task_request_to_dict(task_request)
    
    # Process the task
    await scheduler.process_task("test-task", 50, task_dict)
    
    # Check that the task was re-enqueued with increased priority value (lower priority)
    assert await queue.contains("test-task") is True
    
    # Peek at the queue to check the priority
    result = await queue.peek()
    assert result is not None
    assert result[0] > 50  # Priority value should be increased (lower priority)
    assert result[1] == "test-task"
    
    # Check retry count in the payload
    assert "spec" in result[2]
    assert "retryCount" in result[2]["spec"]
    assert result[2]["spec"]["retryCount"] == 1


@pytest.mark.asyncio
async def test_scheduler_schedule_loop():
    """Test the scheduling loop."""
    # Create a mock queue
    queue = create_queue("memory")
    
    # Create a mock dispatch callback
    dispatch_callback = AsyncMock()
    
    # Create a scheduler
    scheduler = Scheduler(
        queue=queue,
        dispatch_callback=dispatch_callback,
        poll_interval=0.1
    )
    
    # Start the scheduler
    scheduler.running = True
    schedule_task = asyncio.create_task(scheduler.schedule_loop())
    
    # Add tasks to the queue
    for i in range(3):
        task_request = create_task_request(
            name=f"test-task-{i}",
            namespace="default",
            priority=50 + i * 10,
            payload={"test": f"data-{i}"}
        )
        task_dict = task_request_to_dict(task_request)
        
        await queue.enqueue(
            priority=50 + i * 10,
            task_id=f"test-task-{i}",
            payload=task_dict
        )
    
    # Wait for the scheduler to process tasks
    await asyncio.sleep(0.5)
    
    # Stop the scheduler
    scheduler.running = False
    await asyncio.wait_for(schedule_task, timeout=1.0)
    
    # Check that the dispatch callback was called for each task
    assert dispatch_callback.call_count == 3


@pytest.mark.asyncio
async def test_async_scheduler():
    """Test the async scheduler."""
    # Create a mock queue
    queue = create_queue("memory")
    
    # Create a mock dispatch callback
    dispatch_callback = AsyncMock()
    
    # Create an async scheduler
    scheduler = AsyncScheduler(
        queue=queue,
        dispatch_callback=dispatch_callback,
        poll_interval=0.1
    )
    
    # Add a task to the queue
    task_request = create_task_request(
        name="test-task",
        namespace="default",
        priority=50,
        payload={"test": "data"}
    )
    task_dict = task_request_to_dict(task_request)
    
    await queue.enqueue(
        priority=50,
        task_id="test-task",
        payload=task_dict
    )
    
    # Start the scheduler
    scheduler.running = True
    schedule_task = asyncio.create_task(scheduler.schedule_loop())
    
    # Wait for the scheduler to process the task
    await asyncio.sleep(0.3)
    
    # Stop the scheduler
    scheduler.running = False
    await asyncio.wait_for(schedule_task, timeout=1.0)
    
    # Check that the dispatch callback was called
    dispatch_callback.assert_called_once()
