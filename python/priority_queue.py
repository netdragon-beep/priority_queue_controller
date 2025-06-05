"""
Priority Queue Implementation for Kubernetes Task Scheduling

This module implements a priority queue for scheduling tasks in a Kubernetes
environment. It provides both memory-based and Redis-based implementations
to support different deployment scenarios.
"""

import time
import heapq
import asyncio
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple, Union
import redis
import json
import logging

logger = logging.getLogger(__name__)

@dataclass
class QueueItem:
    """Represents an item in the priority queue."""
    priority: int  # Lower values have higher priority
    timestamp: float  # Used as tiebreaker for equal priorities (FIFO)
    task_id: str  # Unique identifier for the task
    payload: Dict[str, Any]  # Task data
    
    def __lt__(self, other):
        """Comparison method used by heapq operations.
        
        Primary sort by priority, secondary sort by timestamp for FIFO behavior.
        """
        if self.priority == other.priority:
            return self.timestamp < other.timestamp
        return self.priority < other.priority


class PriorityQueue:
    """In-memory priority queue implementation using Python's heapq module."""
    
    def __init__(self, max_size: Optional[int] = None):
        """Initialize a new priority queue.
        
        Args:
            max_size: Optional maximum number of items in the queue. None means unlimited.
        """
        self._queue = []  # Underlying heap
        self._lock = asyncio.Lock()  # For thread safety in async contexts
        self._max_size = max_size
        self._item_lookup = {}  # Maps task_id to item for O(1) lookups
    
    async def enqueue(self, priority: int, task_id: str, payload: Dict[str, Any]) -> None:
        """Add a task to the priority queue asynchronously.
        
        Args:
            priority: Priority value (lower is higher priority)
            task_id: Unique identifier for the task
            payload: Task data
            
        Raises:
            ValueError: If the queue is at maximum capacity
        """
        async with self._lock:
            if self._max_size and len(self._queue) >= self._max_size:
                raise ValueError(f"Queue is at maximum capacity: {self._max_size}")
                
            item = QueueItem(
                priority=priority,
                timestamp=time.time(),
                task_id=task_id,
                payload=payload
            )
            
            heapq.heappush(self._queue, item)
            self._item_lookup[task_id] = item
            logger.debug(f"Enqueued task {task_id} with priority {priority}")
    
    async def dequeue(self) -> Optional[Tuple[int, str, Dict[str, Any]]]:
        """Get the highest priority task from the queue asynchronously.
        
        Returns:
            A tuple of (priority, task_id, payload), or None if queue is empty
        """
        async with self._lock:
            if not self._queue:
                return None
                
            item = heapq.heappop(self._queue)
            if item.task_id in self._item_lookup:
                del self._item_lookup[item.task_id]
                
            logger.debug(f"Dequeued task {item.task_id} with priority {item.priority}")
            return (item.priority, item.task_id, item.payload)
            
    async def adjust_priority(self, task_id: str, new_priority: int) -> bool:
        """Adjust the priority of a task in the queue asynchronously.
        
        Args:
            task_id: Unique identifier for the task
            new_priority: New priority value
            
        Returns:
            True if the task was found and priority adjusted, False otherwise
        """
        async with self._lock:
            if task_id not in self._item_lookup:
                return False
                
            # Remove the item from the queue
            self._queue = [item for item in self._queue if item.task_id != task_id]
            heapq.heapify(self._queue)
            
            # Update with new priority and re-add
            item = self._item_lookup[task_id]
            old_priority = item.priority
            item.priority = new_priority
            heapq.heappush(self._queue, item)
            
            logger.debug(f"Adjusted priority for task {task_id} from {old_priority} to {new_priority}")
            return True
    
    async def contains(self, task_id: str) -> bool:
        """Check if a task is in the queue asynchronously.
        
        Args:
            task_id: Unique identifier for the task
            
        Returns:
            True if the task is in the queue, False otherwise
        """
        async with self._lock:
            return task_id in self._item_lookup
    
    async def remove(self, task_id: str) -> bool:
        """Remove a task from the queue asynchronously.
        
        Args:
            task_id: Unique identifier for the task
            
        Returns:
            True if the task was found and removed, False otherwise
        """
        async with self._lock:
            if task_id not in self._item_lookup:
                return False
                
            # Remove the item from the queue
            self._queue = [item for item in self._queue if item.task_id != task_id]
            heapq.heapify(self._queue)
            
            # Remove from lookup
            del self._item_lookup[task_id]
            
            logger.debug(f"Removed task {task_id} from queue")
            return True
    
    def __len__(self) -> int:
        """Get the number of items in the queue."""
        return len(self._queue)
    
    async def peek(self) -> Optional[Tuple[int, str, Dict[str, Any]]]:
        """View the highest priority task without removing it asynchronously.
        
        Returns:
            A tuple of (priority, task_id, payload), or None if queue is empty
        """
        async with self._lock:
            if not self._queue:
                return None
                
            item = self._queue[0]
            return (item.priority, item.task_id, item.payload)
    
    async def dynamic_priority_adjustment(self, age_factor: float = 0.1, max_boost: int = 50) -> int:
        """Adjust priorities based on waiting time to prevent starvation.
        
        Args:
            age_factor: Factor to multiply waiting time by for priority adjustment
            max_boost: Maximum priority boost (reduction in priority value)
            
        Returns:
            Number of tasks that had their priorities adjusted
        """
        async with self._lock:
            if not self._queue:
                return 0
                
            current_time = time.time()
            adjusted_count = 0
            
            for item in self._queue:
                wait_time = current_time - item.timestamp
                # Adjust priority based on waiting time
                priority_boost = min(int(wait_time * age_factor), max_boost)
                
                if priority_boost > 0:
                    original_priority = item.priority
                    item.priority = max(1, original_priority - priority_boost)
                    if item.priority != original_priority:
                        adjusted_count += 1
            
            # Reheapify the queue if any adjustments were made
            if adjusted_count > 0:
                heapq.heapify(self._queue)
                logger.info(f"Adjusted priorities for {adjusted_count} tasks")
                
            return adjusted_count


class RedisPriorityQueue:
    """Redis-backed priority queue implementation for distributed environments."""
    
    def __init__(self, redis_host: str, queue_name: str, ttl: int = 3600):
        """Initialize a new Redis-backed priority queue.
        
        Args:
            redis_host: Redis server hostname or IP
            queue_name: Name of the queue (used as Redis key)
            ttl: Time-to-live for queue items in seconds
        """
        self.redis_host = redis_host
        self.queue_name = queue_name
        self.ttl = ttl
        self.lock_name = f"{queue_name}_lock"
        self.redis_client = redis.StrictRedis(
            host=redis_host.split(':')[0], 
            port=int(redis_host.split(':')[1]) if ':' in redis_host else 6379, 
            db=0
        )
        self._lock = asyncio.Lock()  # Local lock for async operations
    
    async def enqueue(self, priority: int, task_id: str, payload: Dict[str, Any]) -> None:
        """Add a task to the Redis priority queue asynchronously.
        
        Args:
            priority: Priority value (lower is higher priority)
            task_id: Unique identifier for the task
            payload: Task data
        """
        async with self._lock:
            # Store the task payload in Redis with the task_id as key
            payload_key = f"{self.queue_name}:{task_id}"
            self.redis_client.set(
                payload_key, 
                json.dumps(payload),
                ex=self.ttl
            )
            
            # Add the task to the sorted set with priority as score
            self.redis_client.zadd(self.queue_name, {task_id: priority})
            
            # Refresh TTL on the sorted set
            self.redis_client.expire(self.queue_name, self.ttl)
            
            logger.debug(f"Enqueued task {task_id} with priority {priority} in Redis")
    
    async def dequeue(self) -> Optional[Tuple[int, str, Dict[str, Any]]]:
        """Get the highest priority task from the Redis queue asynchronously.
        
        Returns:
            A tuple of (priority, task_id, payload), or None if queue is empty
        """
        async with self._lock:
            # Get the task with the lowest score (highest priority)
            task_with_score = self.redis_client.zrange(
                self.queue_name, 
                0, 0, 
                withscores=True
            )
            
            if not task_with_score:
                return None
            
            task_id_bytes, priority = task_with_score[0]
            task_id = task_id_bytes.decode('utf-8')
            
            # Get the task payload
            payload_key = f"{self.queue_name}:{task_id}"
            payload_json = self.redis_client.get(payload_key)
            
            if not payload_json:
                # Payload missing, remove the task from the queue
                self.redis_client.zrem(self.queue_name, task_id)
                logger.warning(f"Task {task_id} had no payload, removed from queue")
                return None
            
            payload = json.loads(payload_json)
            
            # Remove the task from the queue and delete the payload
            self.redis_client.zrem(self.queue_name, task_id)
            self.redis_client.delete(payload_key)
            
            logger.debug(f"Dequeued task {task_id} with priority {priority} from Redis")
            return (int(priority), task_id, payload)
    
    async def adjust_priority(self, task_id: str, new_priority: int) -> bool:
        """Adjust the priority of a task in the Redis queue asynchronously.
        
        Args:
            task_id: Unique identifier for the task
            new_priority: New priority value
            
        Returns:
            True if the task was found and priority adjusted, False otherwise
        """
        async with self._lock:
            # Check if the task exists in the queue
            current_score = self.redis_client.zscore(self.queue_name, task_id)
            if current_score is None:
                return False
            
            # Update the priority (score)
            self.redis_client.zadd(self.queue_name, {task_id: new_priority})
            
            # Refresh TTL
            self.redis_client.expire(self.queue_name, self.ttl)
            
            logger.debug(f"Adjusted priority for task {task_id} from {current_score} to {new_priority} in Redis")
            return True
    
    async def contains(self, task_id: str) -> bool:
        """Check if a task is in the Redis queue asynchronously.
        
        Args:
            task_id: Unique identifier for the task
            
        Returns:
            True if the task is in the queue, False otherwise
        """
        async with self._lock:
            return self.redis_client.zscore(self.queue_name, task_id) is not None
    
    async def remove(self, task_id: str) -> bool:
        """Remove a task from the Redis queue asynchronously.
        
        Args:
            task_id: Unique identifier for the task
            
        Returns:
            True if the task was found and removed, False otherwise
        """
        async with self._lock:
            # Check if the task exists
            if self.redis_client.zscore(self.queue_name, task_id) is None:
                return False
            
            # Remove the task from the sorted set
            self.redis_client.zrem(self.queue_name, task_id)
            
            # Remove the payload
            payload_key = f"{self.queue_name}:{task_id}"
            self.redis_client.delete(payload_key)
            
            logger.debug(f"Removed task {task_id} from Redis queue")
            return True
    
    def __len__(self) -> int:
        """Get the number of items in the Redis queue."""
        return self.redis_client.zcard(self.queue_name)
    
    async def peek(self) -> Optional[Tuple[int, str, Dict[str, Any]]]:
        """View the highest priority task without removing it asynchronously.
        
        Returns:
            A tuple of (priority, task_id, payload), or None if queue is empty
        """
        async with self._lock:
            # Get the task with the lowest score (highest priority)
            task_with_score = self.redis_client.zrange(
                self.queue_name, 
                0, 0, 
                withscores=True
            )
            
            if not task_with_score:
                return None
            
            task_id_bytes, priority = task_with_score[0]
            task_id = task_id_bytes.decode('utf-8')
            
            # Get the task payload
            payload_key = f"{self.queue_name}:{task_id}"
            payload_json = self.redis_client.get(payload_key)
            
            if not payload_json:
                return None
            
            payload = json.loads(payload_json)
            
            return (int(priority), task_id, payload)
    
    async def dynamic_priority_adjustment(self, age_factor: float = 0.1, max_boost: int = 50) -> int:
        """Adjust priorities based on waiting time to prevent starvation.
        
        This requires additional metadata to track item age, which is not
        directly supported by Redis sorted sets. This implementation uses
        a separate hash to track timestamps.
        
        Args:
            age_factor: Factor to multiply waiting time by for priority adjustment
            max_boost: Maximum priority boost (reduction in priority value)
            
        Returns:
            Number of tasks that had their priorities adjusted
        """
        async with self._lock:
            # Get all tasks with scores
            tasks_with_scores = self.redis_client.zrange(
                self.queue_name, 
                0, -1, 
                withscores=True
            )
            
            if not tasks_with_scores:
                return 0
            
            # Get timestamp metadata for all tasks
            timestamp_key = f"{self.queue_name}_timestamps"
            timestamps = self.redis_client.hgetall(timestamp_key)
            
            current_time = time.time()
            adjusted_count = 0
            
            for task_id_bytes, priority in tasks_with_scores:
                task_id = task_id_bytes.decode('utf-8')
                
                # Get timestamp or use current time if not available
                timestamp_bytes = timestamps.get(task_id_bytes)
                if timestamp_bytes:
                    timestamp = float(timestamp_bytes.decode('utf-8'))
                else:
                    # No timestamp, store current time and continue
                    self.redis_client.hset(timestamp_key, task_id, current_time)
                    continue
                
                # Calculate wait time and priority boost
                wait_time = current_time - timestamp
                priority_boost = min(int(wait_time * age_factor), max_boost)
                
                if priority_boost > 0:
                    new_priority = max(1, int(priority) - priority_boost)
                    if new_priority != int(priority):
                        # Update priority
                        self.redis_client.zadd(self.queue_name, {task_id: new_priority})
                        adjusted_count += 1
            
            # Refresh TTL on timestamps and queue
            if timestamps:
                self.redis_client.expire(timestamp_key, self.ttl)
            self.redis_client.expire(self.queue_name, self.ttl)
            
            if adjusted_count > 0:
                logger.info(f"Adjusted priorities for {adjusted_count} tasks in Redis")
                
            return adjusted_count
    
    def start_promotion(self, interval: int = 60):
        """Start a background thread to periodically adjust priorities.
        
        Args:
            interval: Time between adjustments in seconds
        """
        import threading
        
        def _promotion_loop():
            while True:
                try:
                    # Run dynamic priority adjustment synchronously
                    with self.redis_client.pipeline() as pipe:
                        pipe.watch(self.queue_name)
                        tasks_with_scores = pipe.zrange(
                            self.queue_name, 
                            0, -1, 
                            withscores=True
                        )
                        
                        if not tasks_with_scores:
                            time.sleep(interval)
                            continue
                        
                        # Adjust priorities
                        timestamp_key = f"{self.queue_name}_timestamps"
                        timestamps = pipe.hgetall(timestamp_key)
                        
                        current_time = time.time()
                        adjusted_count = 0
                        
                        pipe.multi()
                        
                        for task_id_bytes, priority in tasks_with_scores:
                            task_id = task_id_bytes.decode('utf-8')
                            
                            # Get timestamp
                            timestamp_bytes = timestamps.get(task_id_bytes)
                            if timestamp_bytes:
                                timestamp = float(timestamp_bytes.decode('utf-8'))
                            else:
                                # No timestamp, store current time and continue
                                pipe.hset(timestamp_key, task_id, current_time)
                                continue
                            
                            # Calculate wait time and priority boost
                            wait_time = current_time - timestamp
                            priority_boost = min(int(wait_time * 0.1), 50)
                            
                            if priority_boost > 0:
                                new_priority = max(1, int(priority) - priority_boost)
                                if new_priority != int(priority):
                                    # Update priority
                                    pipe.zadd(self.queue_name, {task_id: new_priority})
                                    adjusted_count += 1
                        
                        # Refresh TTL
                        pipe.expire(timestamp_key, self.ttl)
                        pipe.expire(self.queue_name, self.ttl)
                        
                        pipe.execute()
                        
                        if adjusted_count > 0:
                            logger.info(f"Adjusted priorities for {adjusted_count} tasks in Redis")
                
                except Exception as e:
                    logger.error(f"Error in priority promotion: {e}")
                
                time.sleep(interval)
        
        promotion_thread = threading.Thread(target=_promotion_loop, daemon=True)
        promotion_thread.start()
        logger.info(f"Started priority promotion thread with interval {interval}s")


# Factory function to create the appropriate queue implementation
def create_queue(queue_type: str, **kwargs) -> Union[PriorityQueue, RedisPriorityQueue]:
    """Create a priority queue of the specified type.
    
    Args:
        queue_type: Type of queue to create ('memory' or 'redis')
        **kwargs: Additional arguments for the queue constructor
        
    Returns:
        A priority queue implementation
        
    Raises:
        ValueError: If an invalid queue type is specified
    """
    if queue_type == 'memory':
        return PriorityQueue(**kwargs)
    elif queue_type == 'redis':
        required_args = ['redis_host', 'queue_name']
        missing_args = [arg for arg in required_args if arg not in kwargs]
        if missing_args:
            raise ValueError(f"Missing required arguments for Redis queue: {missing_args}")
        return RedisPriorityQueue(**kwargs)
    else:
        raise ValueError(f"Invalid queue type: {queue_type}")
