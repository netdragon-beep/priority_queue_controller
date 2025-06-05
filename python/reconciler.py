"""
Reconciler implementation for TaskRequest resources.

This module provides the reconciliation logic for TaskRequest custom resources.
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, Union

from kubernetes import client
from kubernetes.client.rest import ApiException

# Import local modules
from priority_queue import PriorityQueue, RedisPriorityQueue

logger = logging.getLogger(__name__)

class TaskRequestReconciler:
    """Reconciler for TaskRequest custom resources.
    
    This reconciler processes TaskRequest resources and ensures they
    are properly enqueued in the priority queue.
    """
    
    def __init__(
        self,
        client: client.ApiClient,
        queue: Union[PriorityQueue, RedisPriorityQueue],
        group: str = "scheduler.rcme.ai",
        version: str = "v1alpha1",
        plural: str = "taskrequests"
    ):
        """Initialize the reconciler.
        
        Args:
            client: Kubernetes API client
            queue: Priority queue for task scheduling
            group: API group for the TaskRequest CRD
            version: API version for the TaskRequest CRD
            plural: Plural name for the TaskRequest CRD
        """
        self.client = client
        self.queue = queue
        self.group = group
        self.version = version
        self.plural = plural
        self.custom_api = client.CustomObjectsApi(self.client)

    async def reconcile(self, task_request: Dict[str, Any]) -> None:
        """Reconcile a TaskRequest resource.
        
        This method processes a TaskRequest resource and ensures it is
        properly enqueued in the priority queue.
        
        Args:
            task_request: TaskRequest resource data
        """
        task_id = task_request["metadata"]["name"]
        namespace = task_request["metadata"]["namespace"]
        
        # Extract task details
        spec = task_request.get("spec", {})
        priority = spec.get("priority", 100)  # Default priority
        
        # Check if the task is already in the queue
        if not await self.queue.contains(task_id):
            # Add to priority queue
            await self.queue.enqueue(
                priority=priority,
                task_id=task_id,
                payload=task_request
            )
            
            # Update status
            await self.update_task_request_status(
                task_id=task_id,
                namespace=namespace,
                status={"state": "Queued", "queuedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ")}
            )
            
            logger.info(f"Reconciled task {task_id} and added to queue with priority {priority}")
        else:
            # Check if priority has changed
            for item in self.queue.snapshot():
                if item["task_id"] == task_id and item["priority"] != priority:
                    # Update priority
                    await self.queue.adjust_priority(task_id, priority)
                    logger.info(f"Updated priority for task {task_id} to {priority}")
                    break
    
    async def update_task_request_status(self, task_id: str, namespace: str, status: Dict[str, Any]) -> None:
        """Update the status of a TaskRequest in Kubernetes.
        
        Args:
            task_id: Unique identifier for the task
            namespace: Namespace of the task
            status: Status data to update
        """
        try:
            # Get the current TaskRequest
            task_request = self.custom_api.get_namespaced_custom_object(
                group=self.group,
                version=self.version,
                namespace=namespace,
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
                namespace=namespace,
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
    
    def register_with_manager(self, manager):
        """Register the reconciler with a controller manager.
        
        This is a placeholder method for integrating with a controller manager.
        In a real implementation, this would register the reconciler with
        a controller manager that handles reconciliation of resources.
        
        Args:
            manager: Controller manager
        """
        # This is a placeholder for integrating with a controller manager
        # In a real implementation, this would register the reconciler with
        # a controller manager that handles reconciliation of resources
        logger.info(f"Registered TaskRequestReconciler for {self.plural}")
