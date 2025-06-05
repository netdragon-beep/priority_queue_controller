"""
Tests for the HTTP listener implementation.

This module contains tests for the HTTP listener that receives tasks
from external services.
"""

import pytest
import asyncio
from unittest.mock import MagicMock, patch
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from priority_queue import create_queue
from listener import HttpListener


@pytest.mark.asyncio
async def test_handle_task():
    """Test handling of task submission requests."""
    # Create a mock queue
    queue = create_queue("memory")
    
    # Create a listener
    listener = HttpListener(queue=queue, host="localhost", port=8080)
    
    # Create a test client
    app = listener.app
    client = TestClient(TestServer(app))
    await client.start_server()
    
    try:
        # Send a task submission request
        resp = await client.post("/api/tasks", json={
            "priority": 50,
            "payload": {"test": "data"}
        })
        
        # Check response
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "success"
        assert "task_id" in data
        
        # Check that the task was added to the queue
        task_id = data["task_id"]
        assert await queue.contains(task_id) is True
        
        # Peek at the queue to check the task
        result = await queue.peek()
        assert result is not None
        assert result[0] == 50
        assert result[1] == task_id
        assert result[2] == {"test": "data"}
        
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_handle_health():
    """Test handling of health check requests."""
    # Create a mock queue
    queue = create_queue("memory")
    
    # Add some tasks to the queue
    for i in range(3):
        await queue.enqueue(
            priority=100,
            task_id=f"task-{i}",
            payload={"test": f"data-{i}"}
        )
    
    # Create a listener
    listener = HttpListener(queue=queue, host="localhost", port=8080)
    
    # Create a test client
    app = listener.app
    client = TestClient(TestServer(app))
    await client.start_server()
    
    try:
        # Send a health check request
        resp = await client.get("/api/health")
        
        # Check response
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "ok"
        assert data["queue_size"] == 3
        
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_listener_lifecycle():
    """Test listener start and stop."""
    # Create a mock queue
    queue = create_queue("memory")
    
    # Create a listener
    listener = HttpListener(queue=queue, host="localhost", port=8080)
    
    # Start the listener
    await listener.start()
    
    # Stop the listener
    await listener.stop()
    
    # Should not raise any exceptions
    assert True
