"""
Configuration handling for the Kubernetes controller.

This module provides functions for loading and managing configuration
from environment variables and configuration files.
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    "kubernetes": {
        "use_incluster": True,
        "kubeconfig_path": "",
        "namespace": "default"
    },
    "queue": {
        "type": "redis",  # 'redis' or 'memory'
        "redis_host": "redis",
        "redis_port": 6379,
        "queue_name": "taskqueue",
        "ttl": 120,
        "max_size": 10000,
        "default_priority": 100
    },
    "scheduler": {
        "poll_interval_seconds": 1.0,
        "retry_limit": 3,
        "retry_backoff_seconds": 2.0
    },
    "api": {
        "group": "scheduler.rcme.ai",
        "version": "v1alpha1",
        "plural": "taskrequests"
    },
    "promotion": {
        "enabled": True,
        "interval_seconds": 60,
        "age_factor": 0.1,
        "max_boost": 50
    },
    "listener": {
        "enabled": False,
        "mode": "http",  # 'http' or 'grpc'
        "port": 8080
    }
}

def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration from file and environment variables.
    
    Args:
        config_path: Path to the configuration file (YAML)
        
    Returns:
        Configuration dictionary
    """
    # Start with default configuration
    config = DEFAULT_CONFIG.copy()
    
    # Load from file if provided
    if config_path and os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                file_config = yaml.safe_load(f)
                if file_config:
                    # Merge configurations
                    _deep_merge(config, file_config)
                    logger.info(f"Loaded configuration from {config_path}")
        except Exception as e:
            logger.error(f"Error loading configuration from {config_path}: {e}")
    
    # Override with environment variables
    _override_from_env(config)
    
    return config

def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> None:
    """Deep merge override dictionary into base dictionary.
    
    Args:
        base: Base dictionary to merge into
        override: Dictionary with values to override
    """
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value

def _override_from_env(config: Dict[str, Any]) -> None:
    """Override configuration with environment variables.
    
    Environment variables are mapped to configuration keys using the following pattern:
    - CONTROLLER_KUBERNETES_NAMESPACE -> kubernetes.namespace
    - CONTROLLER_QUEUE_TYPE -> queue.type
    - CONTROLLER_SCHEDULER_POLL_INTERVAL_SECONDS -> scheduler.poll_interval_seconds
    
    Args:
        config: Configuration dictionary to update
    """
    prefix = "CONTROLLER_"
    
    for env_key, env_value in os.environ.items():
        if env_key.startswith(prefix):
            # Remove prefix and split by underscore
            key_parts = env_key[len(prefix):].lower().split("_")
            
            if len(key_parts) >= 2:
                # First part is the section, rest is the key with underscores joined by dots
                section = key_parts[0]
                subkey = "_".join(key_parts[1:])
                
                if section in config:
                    if subkey in config[section]:
                        # Convert value to appropriate type
                        if isinstance(config[section][subkey], bool):
                            config[section][subkey] = env_value.lower() in ("true", "1", "yes", "y")
                        elif isinstance(config[section][subkey], int):
                            config[section][subkey] = int(env_value)
                        elif isinstance(config[section][subkey], float):
                            config[section][subkey] = float(env_value)
                        else:
                            config[section][subkey] = env_value
                        
                        logger.debug(f"Overrode {section}.{subkey} with environment variable {env_key}")

def get_redis_address(config: Dict[str, Any]) -> str:
    """Get Redis address from configuration.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Redis address string (host:port)
    """
    host = config["queue"]["redis_host"]
    port = config["queue"]["redis_port"]
    return f"{host}:{port}"
