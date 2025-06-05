"""
Kubernetes controller implementation for managing TaskRequest resources.

This module provides a controller that watches for TaskRequest custom resources
and processes them through a priority queue for scheduling.
"""

import asyncio
import logging
import time
import threading
from typing import Dict, Any, Optional, List, Callable, Awaitable, Union

from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

# Import local modules
from priority_queue import PriorityQueue, RedisPriorityQueue

logger = logging.getLogger(__name__)

class Controller:
    """Kubernetes controller for TaskRequest resources.
    
    This controller watches for TaskRequest custom resources and adds them
    to a priority queue for processing based on their priority.
    """
    
    def __init__(
        self, 
        namespace: str,
        queue: Union[PriorityQueue, RedisPriorityQueue],
        group: str = "scheduler.rcme.ai",
        version: str = "v1alpha1",
        plural: str = "taskrequests"
    ):
        """Initialize the controller.
        
        Args:
            namespace: Kubernetes namespace to watch
            queue: Priority queue for task scheduling
            group: API group for the TaskRequest CRD
            version: API version for the TaskRequest CRD
            plural: Plural name for the TaskRequest CRD
        """
        self.namespace = namespace
        self.queue = queue
        self.group = group
        self.version = version
        self.plural = plural
        self.running = False
        
        # Initialize Kubernetes client
        try:
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes configuration")
        except config.ConfigException:
            config.load_kube_config()
            logger.info("Loaded local Kubernetes configuration")
            
        self.api_client = client.ApiClient()
        self.custom_api = client.CustomObjectsApi(self.api_client)
        
    async def watch_task_requests(self):
        """Watch for TaskRequest CRD events."""
        w = watch.Watch()
        
        while self.running:
            try:
                for event in w.stream(
                    self.custom_api.list_namespaced_custom_object,
                    group=self.group,
                    version=self.version,
                    namespace=self.namespace,
                    plural=self.plural,
                    timeout_seconds=60
                ):
                    event_type = event["type"]
                    task_request = event["object"]
                    
                    await self.handle_task_request_event(event_type, task_request)
                    
            except ApiException as e:
                if e.status == 404:
                    logger.error(f"TaskRequest CRD not found. Make sure it's installed: {e}")
                else:
                    logger.error(f"API error watching TaskRequests: {e}")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Error watching TaskRequests: {e}")
                await asyncio.sleep(5)
                
    async def handle_task_request_event(self, event_type: str, task_request: Dict[str, Any]):
        """Process a TaskRequest event and update the priority queue.
        
        Args:
            event_type: Type of event (ADDED, MODIFIED, DELETED)
            task_request: TaskRequest resource data
        """
        task_id = task_request["metadata"]["name"]
        
        if event_type in ("ADDED", "MODIFIED"):
            # Extract task details
            spec = task_request.get("spec", {})
            priority = spec.get("priority", 100)  # Default priority
            
            # Add to priority queue
            await self.queue.enqueue(
                priority=priority,
                task_id=task_id,
                payload=task_request
            )
            
            # Update status
            await self.update_task_request_status(
                task_id=task_id,
                status={"state": "Queued", "queuedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ")}
            )
            
            logger.info(f"Added task {task_id} to queue with priority {priority}")
            
        elif event_type == "DELETED":
            # Remove from queue if present
            removed = await self.queue.remove(task_id)
            if removed:
                logger.info(f"Removed task {task_id} from queue due to deletion")
            
    async def update_task_request_status(self, task_id: str, status: Dict[str, Any]):
        """Update the status of a TaskRequest in Kubernetes.
        
        Args:
            task_id: Unique identifier for the task
            status: Status data to update
        """
        try:
            # Get the current TaskRequest
            task_request = self.custom_api.get_namespaced_custom_object(
                group=self.group,
                version=self.version,
                namespace=self.namespace,
                plural=self.plural,
                name=task_id
            )
            
            # Update status
            if "status" not in task_request:
                task_request["status"] = {}
                
            task_request["status"].update(status)
            
            # Update in Kubernetes
            self.custom_api.patch_namespaced_custom_object_status(
                group=self.group,
                version=self.version,
                namespace=self.namespace,
                plural=self.plural,
                name=task_id,
                body=task_request
            )
            
            logger.debug(f"Updated status for task {task_id}: {status}")
            
        except ApiException as e:
            if e.status == 404:
                logger.warning(f"TaskRequest {task_id} not found, cannot update status")
            else:
                logger.error(f"API error updating status for task {task_id}: {e}")
        except Exception as e:
            logger.error(f"Error updating status for task {task_id}: {e}")
            
    def start(self):
        """Start the controller in a background thread."""
        self.running = True
        
        async def _run():
            await self.watch_task_requests()
        
        def _thread_target():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_run())
            
        thread = threading.Thread(target=_thread_target, daemon=True)
        thread.start()
        logger.info(f"Started controller watching {self.plural} in namespace {self.namespace}")
        
        return thread
        
    def stop(self):
        """Stop the controller."""
        self.running = False
        logger.info("Stopping controller")


class AsyncController:
    """Asynchronous Kubernetes controller for TaskRequest resources.
    
    This controller is similar to Controller but designed for use in
    asyncio-based applications.
    """
    
    def __init__(
        self, 
        namespace: str,
        queue: Union[PriorityQueue, RedisPriorityQueue],
        group: str = "scheduler.rcme.ai",
        version: str = "v1alpha1",
        plural: str = "taskrequests"
    ):
        """Initialize the controller.
        
        Args:
            namespace: Kubernetes namespace to watch
            queue: Priority queue for task scheduling
            group: API group for the TaskRequest CRD
            version: API version for the TaskRequest CRD
            plural: Plural name for the TaskRequest CRD
        """
        self.namespace = namespace
        self.queue = queue
        self.group = group
        self.version = version
        self.plural = plural
        self.running = False
        
        # Initialize Kubernetes client
        try:
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes configuration")
        except config.ConfigException:
            config.load_kube_config()
            logger.info("Loaded local Kubernetes configuration")
            
        self.api_client = client.ApiClient()
        self.custom_api = client.CustomObjectsApi(self.api_client)
        
    async def watch_task_requests(self):
        """Watch for TaskRequest CRD events."""
        w = watch.Watch()
        
        while self.running:
            try:
                for event in w.stream(
                    self.custom_api.list_namespaced_custom_object,
                    group=self.group,
                    version=self.version,
                    namespace=self.namespace,
                    plural=self.plural,
                    timeout_seconds=60
                ):
                    event_type = event["type"]
                    task_request = event["object"]
                    
                    await self.handle_task_request_event(event_type, task_request)
                    
            except ApiException as e:
                if e.status == 404:
                    logger.error(f"TaskRequest CRD not found. Make sure it's installed: {e}")
                else:
                    logger.error(f"API error watching TaskRequests: {e}")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"Error watching TaskRequests: {e}")
                await asyncio.sleep(5)
                
    async def handle_task_request_event(self, event_type: str, task_request: Dict[str, Any]):
        """Process a TaskRequest event and update the priority queue.
        
        Args:
            event_type: Type of event (ADDED, MODIFIED, DELETED)
            task_request: TaskRequest resource data
        """
        task_id = task_request["metadata"]["name"]
        
        if event_type in ("ADDED", "MODIFIED"):
            # Extract task details
            spec = task_request.get("spec", {})
            priority = spec.get("priority", 100)  # Default priority
            
            # Add to priority queue
            await self.queue.enqueue(
                priority=priority,
                task_id=task_id,
                payload=task_request
            )
            
            # Update status
            await self.update_task_request_status(
                task_id=task_id,
                status={"state": "Queued", "queuedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ")}
            )
            
            logger.info(f"Added task {task_id} to queue with priority {priority}")
            
        elif event_type == "DELETED":
            # Remove from queue if present
            removed = await self.queue.remove(task_id)
            if removed:
                logger.info(f"Removed task {task_id} from queue due to deletion")
            
    async def update_task_request_status(self, task_id: str, status: Dict[str, Any]):
        """Update the status of a TaskRequest in Kubernetes.
        
        Args:
            task_id: Unique identifier for the task
            status: Status data to update
        """
        try:
            # Get the current TaskRequest
            task_request = self.custom_api.get_namespaced_custom_object(
                group=self.group,
                version=self.version,
                namespace=self.namespace,
                plural=self.plural,
                name=task_id
            )
            
            # Update status
            if "status" not in task_request:
                task_request["status"] = {}
                
            task_request["status"].update(status)
            
            # Update in Kubernetes
            self.custom_api.patch_namespaced_custom_object_status(
                group=self.group,
                version=self.version,
                namespace=self.namespace,
                plural=self.plural,
                name=task_id,
                body=task_request
            )
            
            logger.debug(f"Updated status for task {task_id}: {status}")
            
        except ApiException as e:
            if e.status == 404:
                logger.warning(f"TaskRequest {task_id} not found, cannot update status")
            else:
                logger.error(f"API error updating status for task {task_id}: {e}")
        except Exception as e:
            logger.error(f"Error updating status for task {task_id}: {e}")
            
    async def start(self):
        """Start the controller."""
        self.running = True
        await self.watch_task_requests()
        
    async def stop(self):
        """Stop the controller."""
        self.running = False
        logger.info("Stopping controller")


# Interface for listener group integration
class ListenerInterface:
    """Interface for the listener group to submit tasks to the queue.
    
    This class provides methods for external services to submit tasks
    to the priority queue without going through Kubernetes.
    """
    
    def __init__(self, queue: Union[PriorityQueue, RedisPriorityQueue]):
        """Initialize the listener interface.
        
        Args:
            queue: Priority queue for task scheduling
        """
        self.queue = queue
        
    async def submit_task(self, task_id: str, priority: int, payload: Dict[str, Any]) -> bool:
        """Submit a task to the priority queue.
        
        Args:
            task_id: Unique identifier for the task
            priority: Priority value (lower is higher priority)
            payload: Task data
            
        Returns:
            True if the task was successfully submitted
        """
        try:
            await self.queue.enqueue(
                priority=priority,
                task_id=task_id,
                payload=payload
            )
            logger.info(f"Task {task_id} submitted by listener with priority {priority}")
            return True
        except Exception as e:
            logger.error(f"Error submitting task {task_id} from listener: {e}")
            return False
