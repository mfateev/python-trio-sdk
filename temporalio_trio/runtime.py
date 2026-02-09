"""Telemetry and runtime configuration for the Trio-based Temporal SDK.

This module provides configuration dataclasses for metrics export via
Prometheus or OpenTelemetry, matching the official Temporal Python SDK patterns.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from collections.abc import Mapping


@dataclass(frozen=True)
class PrometheusConfig:
    """Configuration for Prometheus metrics export.

    When provided to :py:class:`TelemetryConfig`, starts a Prometheus HTTP
    endpoint that exposes all SDK-Core built-in metrics.

    Args:
        bind_address: Address to bind the Prometheus HTTP server to
            (e.g. ``"0.0.0.0:9090"``).
        counters_total_suffix: If True, append ``_total`` suffix to counter
            metric names.
        unit_suffix: If True, append unit suffix to metric names.
        durations_as_seconds: If True, report durations in seconds instead
            of milliseconds.
    """

    bind_address: str
    counters_total_suffix: bool = False
    unit_suffix: bool = False
    durations_as_seconds: bool = False


@dataclass(frozen=True)
class OpenTelemetryConfig:
    """Configuration for OpenTelemetry metrics export.

    When provided to :py:class:`TelemetryConfig`, sends all SDK-Core built-in
    metrics to an OTLP collector.

    Args:
        url: OTLP collector URL (e.g. ``"http://localhost:4317"``).
        headers: Optional headers to send with OTLP requests.
        metric_periodicity_millis: How often to export metrics in milliseconds.
            Defaults to 1000 (1 second).
        metric_temporality_delta: If True, use delta temporality instead of
            cumulative.
        durations_as_seconds: If True, report durations in seconds instead
            of milliseconds.
        http: If True, use HTTP protocol instead of gRPC.
    """

    url: str
    headers: Mapping[str, str] | None = None
    metric_periodicity_millis: int | None = None
    metric_temporality_delta: bool = False
    durations_as_seconds: bool = False
    http: bool = False


@dataclass(frozen=True)
class TelemetryConfig:
    """Top-level telemetry configuration.

    Controls metrics export and related settings for the Temporal worker runtime.

    Args:
        metrics: Prometheus or OpenTelemetry configuration. If None, no metrics
            are exported (default behavior).
        attach_service_name: If True, attach the service name as a metric label.
        global_tags: Tags applied to all metrics.
        metric_prefix: Prefix for all metric names. Defaults to ``"temporal_"``.
    """

    metrics: PrometheusConfig | OpenTelemetryConfig | None = None
    attach_service_name: bool = True
    global_tags: Mapping[str, str] = field(default_factory=dict)
    metric_prefix: str | None = None

    def _to_json_dict(self) -> dict:
        """Serialize to JSON-compatible dict for bridge transport."""
        d: dict = {
            "attach_service_name": self.attach_service_name,
            "global_tags": dict(self.global_tags) if self.global_tags else None,
            "metric_prefix": self.metric_prefix,
        }
        if isinstance(self.metrics, PrometheusConfig):
            d["prometheus"] = {
                "bind_address": self.metrics.bind_address,
                "counters_total_suffix": self.metrics.counters_total_suffix,
                "unit_suffix": self.metrics.unit_suffix,
                "durations_as_seconds": self.metrics.durations_as_seconds,
            }
        elif isinstance(self.metrics, OpenTelemetryConfig):
            d["opentelemetry"] = {
                "url": self.metrics.url,
                "headers": dict(self.metrics.headers) if self.metrics.headers else {},
                "metric_periodicity_millis": self.metrics.metric_periodicity_millis,
                "metric_temporality_delta": self.metrics.metric_temporality_delta,
                "durations_as_seconds": self.metrics.durations_as_seconds,
                "http": self.metrics.http,
            }
        return d


__all__ = [
    "PrometheusConfig",
    "OpenTelemetryConfig",
    "TelemetryConfig",
]
