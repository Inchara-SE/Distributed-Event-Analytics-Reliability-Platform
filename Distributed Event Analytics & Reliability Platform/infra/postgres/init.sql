-- PostgreSQL metadata initialization for PulseStream
-- Owns application service registry and stream configuration metadata

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS services (
    service_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    service_name VARCHAR(100) NOT NULL UNIQUE,
    display_name VARCHAR(150) NOT NULL,
    owner_team VARCHAR(150),
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_service_slug CHECK (service_name ~ '^[a-z0-9-]+$')
);

CREATE TABLE IF NOT EXISTS stream_settings (
    setting_key VARCHAR(120) PRIMARY KEY,
    setting_value JSONB NOT NULL,
    description TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed application services
INSERT INTO services (service_name, display_name, owner_team, description, is_active)
VALUES
    ('order-service', 'Order Processing Service', 'Checkout Team', 'Manages customer orders, status transitions, and checkout flows.', true),
    ('payment-service', 'Payment Gateway Service', 'Billing Team', 'Integrates with payment processors, tokenization, and settlement.', true),
    ('user-service', 'User Identity Service', 'Identity Team', 'User authentication, profiles, permissions, and sessions.', true),
    ('inventory-service', 'Inventory Management Service', 'Fulfillment Team', 'Real-time item catalog, stock reservation, and warehouse tracking.', true),
    ('notification-service', 'Notification Dispatcher', 'Communications Team', 'Transactional emails, SMS alerts, and mobile push notifications.', true),
    ('gateway-service', 'Edge API Gateway', 'Platform Team', 'Reverse proxy routing, SSL termination, and rate limiting.', true)
ON CONFLICT (service_name) DO UPDATE 
SET display_name = EXCLUDED.display_name,
    owner_team = EXCLUDED.owner_team,
    description = EXCLUDED.description,
    updated_at = NOW();

-- Seed stream configuration settings
INSERT INTO stream_settings (setting_key, setting_value, description)
VALUES
    ('default_topic_events', '"application-events"', 'Default Kafka topic for accepted valid telemetry events.'),
    ('default_topic_dlq', '"application-events-dlq"', 'Dead-letter queue topic for invalid or corrupted broker messages.'),
    ('default_partition_count', '3', 'Number of partitions allocated for telemetry topics.'),
    ('allowed_lateness_minutes', '15', 'Threshold in minutes for automatic real-time minute aggregate correction.'),
    ('deduplication_horizon_days', '7', 'Guaranteed suppression horizon for duplicate logical event IDs.'),
    ('consumer_max_poll_records', '500', 'Maximum batch size polled by event processor consumers.')
ON CONFLICT (setting_key) DO UPDATE
SET setting_value = EXCLUDED.setting_value,
    description = EXCLUDED.description,
    updated_at = NOW();
