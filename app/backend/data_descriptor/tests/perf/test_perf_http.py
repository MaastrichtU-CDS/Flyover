"""
Performance checks of the HTTP layer: per-request overhead, how the
request-boundary work grows with the input, upload and form parsing,
compression, mock-data generation, and the parallel annotation run.

Production runs one gunicorn worker with gevent: a request that keeps the
CPU busy holds up every other request for that long, so the CPU time of a
single request is the figure that matters, not throughput across cores.
"""

import gzip
import io
import threading
import time
import urllib.parse

import brotli
import pytest

from controllers import share_controller
from services import ShareService
from tests.perf.support import (
    JSON,
    TABLE,
    best_of,
    body,
    durations,
    growth_exponent,
    mapping_with_databases,
    mapping_with_variables,
    percentile,
    valid_mapping,
)

try:  # Python 3.14+
    from compression import zstd
except ImportError:  # what Flask-Compress depends on before 3.14
    import pyzstd as zstd


class TestRequestOverhead:
    """Median and 95th-percentile latency of requests the SPA sends often."""

    @pytest.mark.parametrize(
        "method, path, payload, budget_ms",
        [
            ("GET", "/api/check-graph-exists", None, 5),
            ("GET", "/api/v1/suggestions/status", None, 5),
            ("GET", "/describe_landing", None, 5),
            ("GET", "/", None, 10),
            ("POST", "/api/v1/validate-mapping", valid_mapping(), 40),
            ("POST", "/submit-indexeddb-semantic-map", valid_mapping(), 40),
        ],
    )
    def test_requests_the_spa_sends_often_are_cheap(
        self, client, perf, method, path, payload, budget_ms
    ):
        data = body(payload) if payload is not None else None

        measured = durations(
            lambda: client.open(path, method=method, data=data, content_type=JSON),
            repeat=200,
            warmup=10,
        )

        p50, p95 = percentile(measured, 50) * 1000, percentile(measured, 95) * 1000
        perf.record(
            f"{method} {path}",
            p50,
            "ms (p50)",
            f"< {budget_ms} ms",
            f"p95 {p95:.2f} ms",
        )
        assert p50 < budget_ms


class TestHowTheWorkGrows:
    """Growth exponents: about 1 for linear work, 2 for quadratic work."""

    def test_validating_a_mapping_grows_linearly_with_its_variables(
        self, client, store, session_cache, perf
    ):
        sizes = [50, 100, 200, 400]
        session_cache.databases = [TABLE]
        store.get_column_info_by_database.return_value = {
            TABLE: ["id", "sex", "age"] + [f"col_{i:04d}" for i in range(max(sizes))]
        }
        times = []
        for size in sizes:
            payload = body(mapping_with_variables(size))
            times.append(
                best_of(
                    lambda: client.post(
                        "/api/v1/validate-mapping", data=payload, content_type=JSON
                    )
                )
            )

        exponent = growth_exponent(sizes, times)
        perf.record(
            "validate-mapping: time vs variables (50-400)",
            exponent,
            "growth exponent",
            "< 1.5",
            f"{times[-1] * 1000:.0f} ms for 400 variables",
        )
        perf.record(
            "validate-mapping, 400 variables", times[-1] * 1000, "ms", "< 1000 ms"
        )
        assert exponent < 1.5
        assert times[-1] < 1.0

    def test_the_orphan_check_with_many_databases_stays_below_quadratic(
        self, client, store, session_cache, perf
    ):
        # Every mapping database is compared with every loaded database, and
        # every table's columns are gathered from every database's columns:
        # quadratic in the databases, but cheap string comparisons, so the
        # (linear) validation dominates up to a few hundred databases.
        sizes = [25, 50, 100, 200]
        times = []
        for size in sizes:
            mapping = mapping_with_databases(size)
            names = [f"table_{d:04d}" for d in range(size)]
            session_cache.databases = names
            store.get_column_info_by_database.return_value = {
                name: [f"col_{c}" for c in range(10)] for name in names
            }
            payload = body(mapping)
            times.append(
                best_of(
                    lambda: client.post(
                        "/api/v1/validate-mapping", data=payload, content_type=JSON
                    ),
                    repeat=3,
                )
            )

        exponent = growth_exponent(sizes, times)
        perf.record(
            "validate-mapping: time vs databases (25-200)",
            exponent,
            "growth exponent",
            "< 2.0",
            f"{times[1] * 1000:.0f} ms for 50, {times[-1] * 1000:.0f} ms for 200",
        )
        assert exponent < 2.0
        assert times[1] < 0.5

    def test_the_details_state_for_a_wide_mapping(
        self, client, store, session_cache, perf
    ):
        from loaders import JSONLDMapping

        sizes = [75, 150, 300]
        categories = "value,count\n" + "".join(f"code_{i},{i}\n" for i in range(50))
        store.get_categories.return_value = categories
        times, sizes_bytes = [], []
        for size in sizes:
            session_cache.jsonld_mapping = JSONLDMapping.from_dict(
                mapping_with_variables(size)
            )
            store.get_column_info_by_database.return_value = {
                TABLE: [f"col_{i:04d}" for i in range(size)]
            }

            def cold_request():
                session_cache.databases = [TABLE]
                session_cache.descriptive_info = None
                session_cache.DescriptiveInfoDetails = None
                return client.get("/api/v1/describe-variable-details-state")

            times.append(best_of(cold_request, repeat=3))
            sizes_bytes.append(len(cold_request().data))

        exponent = growth_exponent(sizes, times)
        packed = len(
            client.get(
                "/api/v1/describe-variable-details-state",
                headers={"Accept-Encoding": "gzip"},
            ).data
        )
        perf.record(
            "details state: time vs categorical variables (75-300)",
            exponent,
            "growth exponent",
            "< 2.0",
            f"{times[-1] * 1000:.0f} ms and {sizes_bytes[-1] / 1024:.0f} KB "
            f"({packed / 1024:.0f} KB gzip) for 300 variables x 50 values",
        )
        assert exponent < 2.0
        assert times[-1] < 1.0


class TestUploadsAndForms:
    def test_a_large_csv_upload_is_parsed_quickly(self, client, ingest_service, perf):
        payload = b"id,value\n" + b"P0000001,123.456\n" * (2 * 1024 * 1024)
        megabytes = len(payload) / 1024 / 1024
        received = []

        def parse(files, separator, decimal):
            received.append(len(files[0].read()))
            return [], [TABLE], None

        ingest_service.parse_csv_files.side_effect = parse

        elapsed = best_of(
            lambda: client.post(
                "/upload",
                data={"fileType": "CSV", "csvFile": (io.BytesIO(payload), "big.csv")},
            ),
            repeat=3,
        )

        assert received[-1] == len(payload)
        throughput = megabytes / elapsed
        perf.record(
            f"POST /upload, {megabytes:.0f} MB CSV (multipart parsing)",
            throughput,
            "MB/s",
            "> 20 MB/s",
            f"{elapsed * 1000:.0f} ms, including the test client's encoding",
        )
        assert throughput > 20

    def test_the_variables_form_for_300_variables(
        self, client, session_cache, describe_service, perf
    ):
        session_cache.databases = [TABLE]
        fields = {}
        for i in range(300):
            fields[f"{TABLE}_col_{i:04d}"] = "categorical"
            fields[f"ncit_comment_{TABLE}_col_{i:04d}"] = "Some described variable"
            fields[f"comment_{TABLE}_col_{i:04d}"] = "A comment"
        form = urllib.parse.urlencode(fields)

        elapsed = best_of(
            lambda: client.post(
                "/units", data=form, content_type="application/x-www-form-urlencoded"
            )
        )

        perf.record(
            "POST /units, 300 variables (900 fields)",
            elapsed * 1000,
            "ms",
            "< 200 ms",
            f"{len(form) / 1024:.0f} KB of form data",
        )
        assert elapsed < 0.2


class TestCompressionCost:
    def test_compressing_a_large_json_response(self, client, share_service, perf):
        rows = [
            {"id": f"ID_{i:05d}", "sex": "M" if i % 2 else "F", "age": i % 90}
            for i in range(20000)
        ]
        share_service.generate_mock_data_from_semantic_map.return_value = {
            "success": True,
            "data": {f"centre_x.{TABLE}": rows},
            "metadata": {"row_count": len(rows)},
        }
        request = {"jsonld_map": {"x": 1}}
        plain = client.post("/api/generate-mock-data", json=request)
        megabytes = len(plain.data) / 1024 / 1024
        base = best_of(lambda: client.post("/api/generate-mock-data", json=request))

        decompress = {
            "gzip": gzip.decompress,
            "br": brotli.decompress,
            "zstd": zstd.decompress,
        }
        for encoding, unpack in decompress.items():
            headers = {"Accept-Encoding": encoding}
            packed = client.post(
                "/api/generate-mock-data", json=request, headers=headers
            )
            assert unpack(packed.data) == plain.data
            elapsed = best_of(
                lambda: client.post(
                    "/api/generate-mock-data", json=request, headers=headers
                )
            )
            extra_ms_per_mb = max(elapsed - base, 0) * 1000 / megabytes
            ratio = len(plain.data) / len(packed.data)
            perf.record(
                f"compressing {megabytes:.1f} MB of JSON with {encoding}",
                extra_ms_per_mb,
                "ms per MB",
                "< 100 ms per MB",
                f"{ratio:.1f}x smaller",
            )
            assert extra_ms_per_mb < 100
            assert ratio > 3


class TestServiceBoundWork:
    def test_generating_the_largest_mock_dataset_the_page_offers(
        self, client, monkeypatch, perf
    ):
        # The real generator: the endpoint's cost is all in the service.
        monkeypatch.setattr(share_controller, "ShareService", ShareService)
        request = {"jsonld_map": valid_mapping(), "num_rows": 10000, "random_seed": 1}

        response = client.post("/api/generate-mock-data", json=request)
        elapsed = best_of(
            lambda: client.post("/api/generate-mock-data", json=request), repeat=3
        )

        assert response.get_json()["metadata"]["row_count"] == 10000
        perf.record(
            "POST /api/generate-mock-data, 10,000 rows (real generator)",
            elapsed * 1000,
            "ms",
            "< 1000 ms",
            f"{len(response.data) / 1024:.0f} KB of JSON",
        )
        assert elapsed < 1.0


class TestParallelAnnotation:
    def test_databases_are_annotated_in_parallel(
        self, client, session_cache, store, helpers, annotate_service, perf
    ):
        databases = [f"table_{i:02d}" for i in range(64)]
        helpers["has_semantic_map"].return_value = True
        store.get_databases.return_value = databases
        annotate_service.prepare_annotation_data.return_value = {
            name: {"variables": {"v": {}}, "prefixes": ""} for name in databases
        }
        lock, running = threading.Lock(), {"now": 0, "peak": 0}

        def execute(endpoint, database, prefixes, variables, temp_dir):
            with lock:
                running["now"] += 1
                running["peak"] = max(running["peak"], running["now"])
            time.sleep(0.05)  # one store round trip
            with lock:
                running["now"] -= 1
            return {"v": {"success": True}}, None

        annotate_service.execute_annotation.side_effect = execute

        start = time.perf_counter()
        response = client.post("/start-annotation")
        elapsed = time.perf_counter() - start

        assert response.get_json()["success"] is True
        sequential = 64 * 0.05
        perf.record(
            "POST /start-annotation, 64 databases x 50 ms each",
            elapsed * 1000,
            "ms",
            "< 1000 ms",
            f"{sequential / elapsed:.0f}x faster than one at a time; "
            f"{running['peak']} in flight at most",
        )
        assert elapsed < 1.0
        assert running["peak"] <= 32
