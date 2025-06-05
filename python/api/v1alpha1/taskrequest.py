"""
TaskRequest CRD definition for the priority queue controller.

This module defines the Python representation of the TaskRequest custom resource
used by the priority queue controller.
"""

from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field


@dataclass
class TaskRequestSpec:
    """Specification for a TaskRequest resource."""
    
    priority: int = 100  # Default priority (lower values have higher priority)
    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskRequestStatus:
    """Status for a TaskRequest resource."""
    
    state: str = "Pending"  # Initial state
    queuedAt: Optional[str] = None
    startedAt: Optional[str] = None
    completedAt: Optional[str] = None
    message: Optional[str] = None
    retryCount: int = 0


@dataclass
class TaskRequestMetadata:
    """Metadata for a TaskRequest resource."""
    
    name: str
    namespace: str = "default"
    labels: Dict[str, str] = field(default_factory=dict)
    annotations: Dict[str, str] = field(default_factory=dict)
    uid: Optional[str] = None
    resourceVersion: Optional[str] = None
    creationTimestamp: Optional[str] = None


@dataclass
class TaskRequest:
    """TaskRequest custom resource definition."""
    
    apiVersion: str = "scheduler.rcme.ai/v1alpha1"
    kind: str = "TaskRequest"
    metadata: TaskRequestMetadata = field(default_factory=TaskRequestMetadata)
    spec: TaskRequestSpec = field(default_factory=TaskRequestSpec)
    status: Optional[TaskRequestStatus] = None


def create_task_request(
    name: str,
    namespace: str = "default",
    priority: int = 100,
    payload: Dict[str, Any] = None
) -> TaskRequest:
    """Create a new TaskRequest object.
    
    Args:
        name: Name of the TaskRequest
        namespace: Kubernetes namespace
        priority: Task priority (lower values have higher priority)
        payload: Task payload data
        
    Returns:
        A TaskRequest object
    """
    if payload is None:
        payload = {}
        
    metadata = TaskRequestMetadata(
        name=name,
        namespace=namespace
    )
    
    spec = TaskRequestSpec(
        priority=priority,
        payload=payload
    )
    
    return TaskRequest(
        metadata=metadata,
        spec=spec
    )


def task_request_to_dict(task_request: TaskRequest) -> Dict[str, Any]:
    """Convert a TaskRequest object to a dictionary.
    
    Args:
        task_request: TaskRequest object
        
    Returns:
        Dictionary representation of the TaskRequest
    """
    result = {
        "apiVersion": task_request.apiVersion,
        "kind": task_request.kind,
        "metadata": {
            "name": task_request.metadata.name,
            "namespace": task_request.metadata.namespace
        },
        "spec": {
            "priority": task_request.spec.priority,
            "payload": task_request.spec.payload
        }
    }
    
    # Add optional metadata fields if present
    if task_request.metadata.labels:
        result["metadata"]["labels"] = task_request.metadata.labels
    if task_request.metadata.annotations:
        result["metadata"]["annotations"] = task_request.metadata.annotations
    if task_request.metadata.uid:
        result["metadata"]["uid"] = task_request.metadata.uid
    if task_request.metadata.resourceVersion:
        result["metadata"]["resourceVersion"] = task_request.metadata.resourceVersion
    if task_request.metadata.creationTimestamp:
        result["metadata"]["creationTimestamp"] = task_request.metadata.creationTimestamp
    
    # Add status if present
    if task_request.status:
        result["status"] = {
            "state": task_request.status.state
        }
        if task_request.status.queuedAt:
            result["status"]["queuedAt"] = task_request.status.queuedAt
        if task_request.status.startedAt:
            result["status"]["startedAt"] = task_request.status.startedAt
        if task_request.status.completedAt:
            result["status"]["completedAt"] = task_request.status.completedAt
        if task_request.status.message:
            result["status"]["message"] = task_request.status.message
        if task_request.status.retryCount:
            result["status"]["retryCount"] = task_request.status.retryCount
    
    return result


def dict_to_task_request(data: Dict[str, Any]) -> TaskRequest:
    """Convert a dictionary to a TaskRequest object.
    
    Args:
        data: Dictionary representation of the TaskRequest
        
    Returns:
        TaskRequest object
    """
    metadata_dict = data.get("metadata", {})
    metadata = TaskRequestMetadata(
        name=metadata_dict.get("name", ""),
        namespace=metadata_dict.get("namespace", "default"),
        labels=metadata_dict.get("labels", {}),
        annotations=metadata_dict.get("annotations", {}),
        uid=metadata_dict.get("uid"),
        resourceVersion=metadata_dict.get("resourceVersion"),
        creationTimestamp=metadata_dict.get("creationTimestamp")
    )
    
    spec_dict = data.get("spec", {})
    spec = TaskRequestSpec(
        priority=spec_dict.get("priority", 100),
        payload=spec_dict.get("payload", {})
    )
    
    status = None
    if "status" in data:
        status_dict = data["status"]
        status = TaskRequestStatus(
            state=status_dict.get("state", "Pending"),
            queuedAt=status_dict.get("queuedAt"),
            startedAt=status_dict.get("startedAt"),
            completedAt=status_dict.get("completedAt"),
            message=status_dict.get("message"),
            retryCount=status_dict.get("retryCount", 0)
        )
    
    return TaskRequest(
        apiVersion=data.get("apiVersion", "scheduler.rcme.ai/v1alpha1"),
        kind=data.get("kind", "TaskRequest"),
        metadata=metadata,
        spec=spec,
        status=status
    )
