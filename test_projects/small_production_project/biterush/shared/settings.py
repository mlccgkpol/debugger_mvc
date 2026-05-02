"""
shared/settings.py

Pydantic-settings based config loader for BiteRush microservices.
Each service imports and extends BaseServiceSettings.
"""

import os
from pydantic_settings import BaseSettings


class BaseServiceSettings(BaseSettings):
    service_name: str = "biterush-service"
    environment: str = os.getenv("ENVIRONMENT", "production")
    debug: bool = False

    db_dsn: str = ""
    db_pool_size: int = 10
    db_acquire_timeout: float = 3.0

    redis_host: str = "redis-primary.biterush.svc.cluster.local"
    redis_port: int = 6379

    kafka_brokers: str = "kafka-1.biterush.svc:9092,kafka-2.biterush.svc:9092"
    kafka_group_prefix: str = "biterush"

    jwt_secret: str = os.getenv("BITERUSH_JWT_SECRET", "biterush-prod-hs256-fallback")
    jwt_expiry_seconds: int = 7200

    internal_timeout: float = 5.0

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
