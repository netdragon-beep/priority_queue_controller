"""
Main entry point for the Kubernetes controller with priority queue.

This module initializes and starts the controller and scheduler components.
"""

import os
import asyncio
import logging
import signal
import time
from typing import Dict, Any

from kubernetes import client, config

# Import local modules
from priority_queue import create_queue, RedisPriorityQueue
from controller import Controller
from scheduler import Scheduler, dispatch_to_llm, start_promotion

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global references for cleanup
controller_thread = None
scheduler_thread = None

def load_config() -> Dict[str, Any]:
    """Load configuration from environment variables."""
    config = {
        "redis_addr": os.getenv("REDIS_ADDR", "redis:6379"),
        "queue_name": os.getenv("QUEUE_NAME", "taskqueue"),
        "queue_ttl": int(os.getenv("QUEUE_TTL", "120")),
        "namespace": os.getenv("KUBERNETES_NAMESPACE", "default"),
        "poll_interval": float(os.getenv("POLL_INTERVAL", "1.0")),
        "group": os.getenv("API_GROUP", "scheduler.rcme.ai"),
        "version": os.getenv("API_VERSION", "v1alpha1"),
        "plural": os.getenv("API_PLURAL", "taskrequests"),
        "max_retries": int(os.getenv("MAX_RETRIES", "3")),
        "promotion_interval": int(os.getenv("PROMOTION_INTERVAL", "60")),
        "queue_type": os.getenv("QUEUE_TYPE", "redis"),  # 'redis' or 'memory'
    }
    return config

def main():
    """Main entry point for the application."""
    global controller_thread, scheduler_thread
    
    # Load configuration
    cfg = load_config()
    
    # Initialize Kubernetes client
    try:
        config.load_incluster_config()
        logger.info("Loaded in-cluster Kubernetes configuration")
    except config.ConfigException:
        config.load_kube_config()
        logger.info("Loaded local Kubernetes configuration")
    
    k8s_client = client.ApiClient()
    
    # Create the priority queue
    if cfg["queue_type"] == "redis":
        queue = create_queue(
            "redis",
            redis_host=cfg["redis_addr"],
            queue_name=cfg["queue_name"],
            ttl=cfg["queue_ttl"]
        )
    else:
        queue = create_queue("memory")
    
    # Start priority promotion for Redis queue
    if cfg["queue_type"] == "redis" and isinstance(queue, RedisPriorityQueue):
        start_promotion(queue, cfg["promotion_interval"])
    
    # Create and start the controller
    controller = Controller(
        namespace=cfg["namespace"],
        queue=queue,
        group=cfg["group"],
        version=cfg["version"],
        plural=cfg["plural"]
    )
    controller_thread = controller.start()
    
    # Create and start the scheduler
    scheduler = Scheduler(
        queue=queue,
        dispatch_callback=dispatch_to_llm,
        poll_interval=cfg["poll_interval"],
        max_retries=cfg["max_retries"]
    )
    scheduler_thread = scheduler.start()
    
    logger.info("Priority queue controller started")
    
    # Wait for signals to gracefully shutdown
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
        controller.stop()
        scheduler.stop()
        logger.info("Controller and scheduler stopped")

if __name__ == "__main__":
    main()
