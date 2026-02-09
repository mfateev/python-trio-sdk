"""Unit tests for telemetry configuration dataclasses."""

from temporalio_trio.runtime import (
    OpenTelemetryConfig,
    PrometheusConfig,
    TelemetryConfig,
)


class TestPrometheusConfig:
    def test_basic_config(self):
        config = PrometheusConfig(bind_address="0.0.0.0:9090")
        assert config.bind_address == "0.0.0.0:9090"
        assert config.counters_total_suffix is False
        assert config.unit_suffix is False
        assert config.durations_as_seconds is False

    def test_all_options(self):
        config = PrometheusConfig(
            bind_address="127.0.0.1:8080",
            counters_total_suffix=True,
            unit_suffix=True,
            durations_as_seconds=True,
        )
        assert config.bind_address == "127.0.0.1:8080"
        assert config.counters_total_suffix is True
        assert config.unit_suffix is True
        assert config.durations_as_seconds is True

    def test_frozen(self):
        config = PrometheusConfig(bind_address="0.0.0.0:9090")
        try:
            config.bind_address = "new"  # type: ignore[misc]
            assert False, "Should have raised"
        except AttributeError:
            pass


class TestOpenTelemetryConfig:
    def test_basic_config(self):
        config = OpenTelemetryConfig(url="http://localhost:4317")
        assert config.url == "http://localhost:4317"
        assert config.headers is None
        assert config.metric_periodicity_millis is None
        assert config.metric_temporality_delta is False
        assert config.durations_as_seconds is False
        assert config.http is False

    def test_all_options(self):
        config = OpenTelemetryConfig(
            url="http://collector:4318",
            headers={"Authorization": "Bearer token"},
            metric_periodicity_millis=5000,
            metric_temporality_delta=True,
            durations_as_seconds=True,
            http=True,
        )
        assert config.url == "http://collector:4318"
        assert config.headers == {"Authorization": "Bearer token"}
        assert config.metric_periodicity_millis == 5000
        assert config.metric_temporality_delta is True
        assert config.durations_as_seconds is True
        assert config.http is True


class TestTelemetryConfig:
    def test_defaults(self):
        config = TelemetryConfig()
        assert config.metrics is None
        assert config.attach_service_name is True
        assert config.global_tags == {}
        assert config.metric_prefix is None

    def test_no_metrics_to_json(self):
        config = TelemetryConfig()
        d = config._to_json_dict()
        assert d["attach_service_name"] is True
        assert d["global_tags"] is None
        assert d["metric_prefix"] is None
        assert "prometheus" not in d
        assert "opentelemetry" not in d

    def test_prometheus_to_json(self):
        config = TelemetryConfig(
            metrics=PrometheusConfig(
                bind_address="0.0.0.0:9090",
                counters_total_suffix=True,
            ),
            metric_prefix="myapp_",
            global_tags={"env": "prod"},
        )
        d = config._to_json_dict()
        assert d["attach_service_name"] is True
        assert d["metric_prefix"] == "myapp_"
        assert d["global_tags"] == {"env": "prod"}
        assert "opentelemetry" not in d

        prom = d["prometheus"]
        assert prom["bind_address"] == "0.0.0.0:9090"
        assert prom["counters_total_suffix"] is True
        assert prom["unit_suffix"] is False
        assert prom["durations_as_seconds"] is False

    def test_otel_to_json(self):
        config = TelemetryConfig(
            metrics=OpenTelemetryConfig(
                url="http://localhost:4317",
                headers={"X-Key": "val"},
                metric_periodicity_millis=2000,
                metric_temporality_delta=True,
                http=True,
            ),
            attach_service_name=False,
        )
        d = config._to_json_dict()
        assert d["attach_service_name"] is False
        assert "prometheus" not in d

        otel = d["opentelemetry"]
        assert otel["url"] == "http://localhost:4317"
        assert otel["headers"] == {"X-Key": "val"}
        assert otel["metric_periodicity_millis"] == 2000
        assert otel["metric_temporality_delta"] is True
        assert otel["durations_as_seconds"] is False
        assert otel["http"] is True

    def test_otel_no_headers_to_json(self):
        config = TelemetryConfig(
            metrics=OpenTelemetryConfig(url="http://localhost:4317"),
        )
        d = config._to_json_dict()
        otel = d["opentelemetry"]
        assert otel["headers"] == {}
        assert otel["metric_periodicity_millis"] is None

    def test_global_tags_empty_to_json(self):
        config = TelemetryConfig(global_tags={})
        d = config._to_json_dict()
        assert d["global_tags"] is None
