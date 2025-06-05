"""
Scheduler implementation for processing tasks from the priority queue.

This module provides schedulers that dequeue tasks from the priority queue
and dispatch them to the appropriate backend services.
"""

import asyncio
import logging
import time
import threading
from typing import Dict, Any, Optional, Callable, Awaitable, Union

from kubernetes import client, config
from kubernetes.client.rest import ApiException

# Import local modules
from priority_queue import PriorityQueue, RedisPriorityQueue

logger = logging.getLogger(__name__)

class Scheduler:
    """Scheduler for processing tasks from the priority queue.
    
    This scheduler dequeues tasks from the priority queue and dispatches
    them to the appropriate backend services.
    """
    
    def __init__(
        self,
        queue: Union[PriorityQueue, RedisPriorityQueue],
        dispatch_callback: Callable[[str, Dict[str, Any]], Any],
        poll_interval: float = 1.0,
        max_retries: int = 3,
        retry_delay: float = 2.0
    ):
        """Initialize the scheduler.
        
        Args:
            queue: Priority queue for task scheduling
            dispatch_callback: Callback function for dispatching tasks
            poll_interval: Interval in seconds between polling the queue
            max_retries: Maximum number of retry attempts for failed tasks
            retry_delay: Delay in seconds between retry attempts
        """
        self.queue = queue
        self.dispatch_callback = dispatch_callback
        self.poll_interval = poll_interval
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.running = False
        
    async def schedule_loop(self):
        """Main scheduling loop that processes tasks from the queue."""
        logger.info("Starting scheduler loop")
        
        while self.running:
            try:
                # Get the highest priority task
                result = await self.queue.dequeue()
                
                if result:
                    priority, task_id, payload = result
                    await self.process_task(task_id, priority, payload)
                else:
                    # Queue is empty, wait before checking again
                    await asyncio.sleep(self.poll_interval)
                    
            except Exception as e:
                logger.error(f"Error in scheduling loop: {e}")
                await asyncio.sleep(self.poll_interval)
                
    async def process_task(self, task_id: str, priority: int, payload: Dict[str, Any]):
        """Process a task from the queue.
        
        Args:
            task_id: Unique identifier for the task
            priority: Priority value of the task
            payload: Task data
        """
        try:
            logger.info(f"Processing task {task_id} with priority {priority}")
            
            # Extract retry count from payload if available
            spec = payload.get("spec", {})
            retry_count = spec.get("retryCount", 0)
            
            # Call the dispatch callback with the task
            await self.dispatch_callback(task_id, payload)
            
            logger.info(f"Task {task_id} processed successfully")
            
        except Exception as e:
            logger.error(f"Error processing task {task_id}: {e}")
            
            # Check if we should retry
            if retry_count < self.max_retries:
                # Increment retry count
                retry_count += 1
                
                # Adjust priority for retry (lower priority by increasing the value)
                new_priority = priority + 50
                
                # Update payload for retry
                if "spec" not in payload:
                    payload["spec"] = {}
                payload["spec"]["retryCount"] = retry_count
                payload["spec"]["priority"] = new_priority
                
                # Wait before re-enqueueing
                await asyncio.sleep(self.retry_delay * retry_count)
                
                # Re-enqueue with adjusted priority
                await self.queue.enqueue(
                    priority=new_priority,
                    task_id=task_id,
                    payload=payload
                )
                
                logger.info(f"Re-enqueued task {task_id} for retry {retry_count}/{self.max_retries} with new priority {new_priority}")
            else:
                logger.warning(f"Task {task_id} exceeded maximum retry attempts ({self.max_retries})")
                
                # Could implement a dead-letter queue or failure notification here
                
    def start(self):
        """Start the scheduler in a background thread."""
        self.running = True
        
        async def _run():
            await self.schedule_loop()
        
        def _thread_target():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_run())
            
        thread = threading.Thread(target=_thread_target, daemon=True)
        thread.start()
        logger.info("Started scheduler")
        
        return thread
        
    def stop(self):
        """Stop the scheduler."""
        self.running = False
        logger.info("Stopping scheduler")


class AsyncScheduler:
    """Asynchronous scheduler for processing tasks from the priority queue.
    
    This scheduler is similar to Scheduler but designed for use in
    asyncio-based applications.
    """
    
    def __init__(
        self,
        queue: Union[PriorityQueue, RedisPriorityQueue],
        dispatch_callback: Callable[[str, Dict[str, Any]], Awaitable[Any]],
        poll_interval: float = 1.0,
        max_retries: int = 3,
        retry_delay: float = 2.0
    ):
        """Initialize the scheduler.
        
        Args:
            queue: Priority queue for task scheduling
            dispatch_callback: Async callback function for dispatching tasks
            poll_interval: Interval in seconds between polling the queue
            max_retries: Maximum number of retry attempts for failed tasks
            retry_delay: Delay in seconds between retry attempts
        """
        self.queue = queue
        self.dispatch_callback = dispatch_callback
        self.poll_interval = poll_interval
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.running = False
        
    async def schedule_loop(self):
        """Main scheduling loop that processes tasks from the queue."""
        logger.info("Starting async scheduler loop")
        
        while self.running:
            try:
                # Get the highest priority task
                result = await self.queue.dequeue()
                
                if result:
                    priority, task_id, payload = result
                    await self.process_task(task_id, priority, payload)
                else:
                    # Queue is empty, wait before checking again
                    await asyncio.sleep(self.poll_interval)
                    
            except Exception as e:
                logger.error(f"Error in async scheduling loop: {e}")
                await asyncio.sleep(self.poll_interval)
                
    async def process_task(self, task_id: str, priority: int, payload: Dict[str, Any]):
        """Process a task from the queue.
        
        Args:
            task_id: Unique identifier for the task
            priority: Priority value of the task
            payload: Task data
        """
        try:
            logger.info(f"Processing task {task_id} with priority {priority}")
            
            # Extract retry count from payload if available
            spec = payload.get("spec", {})
            retry_count = spec.get("retryCount", 0)
            
            # Call the dispatch callback with the task
            await self.dispatch_callback(task_id, payload)
            
            logger.info(f"Task {task_id} processed successfully")
            
        except Exception as e:
            logger.error(f"Error processing task {task_id}: {e}")
            
            # Check if we should retry
            if retry_count < self.max_retries:
                # Increment retry count
                retry_count += 1
                
                # Adjust priority for retry (lower priority by increasing the value)
                new_priority = priority + 50
                
                # Update payload for retry
                if "spec" not in payload:
                    payload["spec"] = {}
                payload["spec"]["retryCount"] = retry_count
                payload["spec"]["priority"] = new_priority
                
                # Wait before re-enqueueing
                await asyncio.sleep(self.retry_delay * retry_count)
                
                # Re-enqueue with adjusted priority
                await self.queue.enqueue(
                    priority=new_priority,
                    task_id=task_id,
                    payload=payload
                )
                
                logger.info(f"Re-enqueued task {task_id} for retry {retry_count}/{self.max_retries} with new priority {new_priority}")
            else:
                logger.warning(f"Task {task_id} exceeded maximum retry attempts ({self.max_retries})")
                
                # Could implement a dead-letter queue or failure notification here
                
    async def start(self):
        """Start the scheduler."""
        self.running = True
        await self.schedule_loop()
        
    async def stop(self):
        """Stop the scheduler."""
        self.running = False
        logger.info("Stopping async scheduler")


# Example dispatch function for LLM processing
async def dispatch_to_llm(task_id: str, payload: Dict[str, Any]) -> None:
    """Dispatch a task to an LLM service.
    
    This is a placeholder implementation that should be replaced with
    actual code to send tasks to your LLM backend.
    
    Args:
        task_id: Unique identifier for the task
        payload: Task data
    """
    logger.info(f"Dispatching task {task_id} to LLM service")
    
    # In a real implementation, this would call an external service
    # For example:
    # async with aiohttp.ClientSession() as session:
    #     async with session.post("http://llm-service:8000/api/tasks", json=payload) as resp:
    #         result = await resp.json()
    #         if resp.status != 200:
    #             raise Exception(f"LLM service returned error: {result}")
    
    # Simulate processing time
    await asyncio.sleep(1)
    
    logger.info(f"Task {task_id} processed successfully by LLM service")


# Function to start the promotion thread for Redis queue
def start_promotion(queue: RedisPriorityQueue, interval: int = 60) -> None:
    """Start the priority promotion thread for a Redis queue.
    
    Args:
        queue: Redis priority queue
        interval: Interval in seconds between priority adjustments
    """
    if isinstance(queue, RedisPriorityQueue):
        queue.start_promotion(interval)
        logger.info(f"Started priority promotion with interval {interval}s")
    else:
        logger.warning("Priority promotion is only available for Redis queues")


# Function to start the dispatcher
def start_dispatcher(
    client: client.ApiClient,
    queue: Union[PriorityQueue, RedisPriorityQueue],
    poll_interval: float = 1.0
) -> threading.Thread:
    """Start the dispatcher in a background thread.
    
    Args:
        client: Kubernetes API client
        queue: Priority queue for task scheduling
        poll_interval: Interval in seconds between polling the queue
        
    Returns:
        The background thread running the dispatcher
    """
    scheduler = Scheduler(
        queue=queue,
        dispatch_callback=dispatch_to_llm,
        poll_interval=poll_interval
    )
    
    thread = scheduler.start()
    return thread
