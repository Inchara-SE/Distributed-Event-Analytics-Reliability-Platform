"""PulseStream configuration management using Pydantic Settings.

Reads configuration from environment variables and optional .env file.
Provides validated, strongly-typed settings across API, consumers, producer, and storage.
"""

from typing import Literal
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # General / Application
    env: Literal["development", "test", "production"] = "development"
    log_level: str = "INFO"
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # Redpanda / Kafka Broker
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_events: str = "application-events"
    kafka_topic_dlq: str = "application-events-dlq"
    kafka_partitions: int = 3
    kafka_replication_factor: int = 1
    kafka_producer_acks: str = "all"
    kafka_producer_idempotence: bool = True
    kafka_delivery_timeout_ms: int = 10000

    # Consumer Groups
    consumer_group_events: str = "pulsestream-event-processors"
    consumer_group_dlq: str = "pulsestream-dlq-archiver"
    consumer_max_poll_records: int = 500
    consumer_auto_commit: bool = False
    consumer_poll_timeout_ms: int = 1000

    # ClickHouse
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_native_port: int = 9000
    clickhouse_db: str = "pulsestream"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "pulsestream_metadata"
    postgres_user: str = "pulsestream"
    postgres_password: str = "pulsestream_dev_password"

    # Analytics / dbt Policies
    analytics_cadence_seconds: int = 60
    allowed_lateness_minutes: int = 15
    dedup_horizon_days: int = 7

    # Observability
    prometheus_port: int = 9090
    grafana_port: int = 3000


settings = Settings()
