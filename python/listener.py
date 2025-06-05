"""
HTTP listener for receiving tasks from external services.

This module provides an HTTP server that allows external services
to submit tasks directly to the priority queue without going through
Kubernetes.
"""

import asyncio
import logging
import json
import uuid
from typing import Dict, Any, Optional, Union
from aiohttp import web

from priority_queue import PriorityQueue, RedisPriorityQueue
from api.v1alpha1.taskrequest import create_task_request, task_request_to_dict

logger = logging.getLogger(__name__)

class HttpListener:
    """HTTP listener for receiving tasks from external services."""
    
    def __init__(
        self,
        queue: Union[PriorityQueue, RedisPriorityQueue],
        host: str = "0.0.0.0",
        port: int = 8080
    ):
        """Initialize the HTTP listener.
        
        Args:
            queue: Priority queue for tasks
            host: Host to bind to
            port: Port to listen on
        """
        self.queue = queue
        self.host = host
        self.port = port
        self.app = web.Application()
        self.app.router.add_post("/api/tasks", self.handle_task)
        self.app.router.add_get("/api/health", self.handle_health)
        self.runner = None
        self.site = None
        
    async def handle_task(self, request: web.Request) -> web.Response:
        """Handle a task submission request.
        
        Args:
            request: HTTP request
            
        Returns:
            HTTP response
        """
        try:
            # Parse request body
            body = await request.json()
            
            # Extract task details
            priority = body.get("priority", 100)
            payload = body.get("payload", {})
            task_id = body.get("task_id", str(uuid.uuid4()))
            
            # Add to priority queue
            await self.queue.enqueue(
                priority=priority,
                task_id=task_id,
                payload=payload
            )
            
            logger.info(f"Added task {task_id} to queue with priority {priority} via HTTP")
            
            # Return success response
            return web.json_response({
                "status": "success",
                "task_id": task_id,
                "message": "Task added to queue"
            })
            
        except Exception as e:
            logger.error(f"Error handling task submission: {e}")
            
            # Return error response
            return web.json_response({
                "status": "error",
                "message": str(e)
            }, status=500)
    
    async def handle_health(self, request: web.Request) -> web.Response:
        """Handle a health check request.
        
        Args:
            request: HTTP request
            
        Returns:
            HTTP response
        """
        return web.json_response({
            "status": "ok",
            "queue_size": len(self.queue)
        })
    
    async def start(self):
        """Start the HTTP listener."""
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()
        logger.info(f"Started HTTP listener on {self.host}:{self.port}")
    
    async def stop(self):
        """Stop the HTTP listener."""
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
        logger.info("Stopped HTTP listener")
