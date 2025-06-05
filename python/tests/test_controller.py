"""
Tests for the Kubernetes controller implementation.

This module contains tests for the controller that watches TaskRequest resources.
"""

import pytest
import asyncio
import time
import json
from unittest.mock import MagicMock, patch

from kubernetes.client.rest import ApiException
from priority_queue import create_queue
from controller import Controller, ListenerInterface
from api.v1alpha1.taskrequest import create_task_request, task_request_to_dict


@pytest.mark.asyncio
async def test_handle_task_request_added_event():
    """Test handling of ADDED events for TaskRequests."""
    # Create a mock queue
    queue = create_queue("memory")
    
    # Create a mock controller with mocked API
    controller = Controller(namespace="default", queue=queue)
    controller.custom_api = MagicMock()
    controller.update_task_request_status = MagicMock()
    
    # Create a sample task request
    task_request = create_task_request(
        name="test-task",
        namespace="default",
        priority=50,
        payload={"test": "data"}
    )
    task_dict = task_request_to_dict(task_request)
    
    # Handle the event
    await controller.handle_task_request_event("ADDED", task_dict)
    
    # Check that the task was added to the queue
    assert await queue.contains("test-task") is True
    
    # Check that status was updated
    controller.update_task_request_status.assert_called_once()


@pytest.mark.asyncio
async def test_handle_task_request_modified_event():
    """Test handling of MODIFIED events for TaskRequests."""
    # Create a mock queue
    queue = create_queue("memory")
    
    # Create a mock controller with mocked API
    controller = Controller(namespace="default", queue=queue)
    controller.custom_api = MagicMock()
    controller.update_task_request_status = MagicMock()
    
    # Create a sample task request
    task_request = create_task_request(
        name="test-task",
        namespace="default",
        priority=100,
        payload={"test": "data"}
    )
    task_dict = task_request_to_dict(task_request)
    
    # Add the task to the queue
    await queue.enqueue(
        priority=100,
        task_id="test-task",
        payload=task_dict
    )
    
    # Update the task priority
    task_request.spec.priority = 50
    updated_task_dict = task_request_to_dict(task_request)
    
    # Handle the event
    await controller.handle_task_request_event("MODIFIED", updated_task_dict)
    
    # Peek at the queue to check the priority
    result = await queue.peek()
    assert result is not None
    assert result[0] == 50  # Priority should be updated


@pytest.mark.asyncio
async def test_handle_task_request_deleted_event():
    """Test handling of DELETED events for TaskRequests."""
    # Create a mock queue
    queue = create_queue("memory")
    
    # Create a mock controller with mocked API
    controller = Controller(namespace="default", queue=queue)
    controller.custom_api = MagicMock()
    
    # Create a sample task request
    task_request = create_task_request(
        name="test-task",
        namespace="default",
        priority=100,
        payload={"test": "data"}
    )
    task_dict = task_request_to_dict(task_request)
    
    # Add the task to the queue
    await queue.enqueue(
        priority=100,
        task_id="test-task",
        payload=task_dict
    )
    
    # Handle the event
    await controller.handle_task_request_event("DELETED", task_dict)
    
    # Check that the task was removed from the queue
    assert await queue.contains("test-task") is False


@pytest.mark.asyncio
async def test_listener_interface():
    """Test the listener interface for submitting tasks."""
    # Create a mock queue
    queue = create_queue("memory")
    
    # Create a listener interface
    listener = ListenerInterface(queue)
    
    # Submit a task
    success = await listener.submit_task(
        task_id="test-task",
        priority=50,
        payload={"test": "data"}
    )
    
    # Check that the task was submitted successfully
    assert success is True
    assert await queue.contains("test-task") is True
    
    # Peek at the queue to check the task
    result = await queue.peek()
    assert result is not None
    assert result[0] == 50
    assert result[1] == "test-task"
    assert result[2] == {"test": "data"}
