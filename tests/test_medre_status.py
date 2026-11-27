"""Actual status parser and API method with isolated HTTP boundaries."""
from __future__ import annotations

import json
import runpy
import time
import unittest
from pathlib import Path

from test_installed_api_behavior import client_namespace

COMPONENT = Path(__file__).resolve().parents[1] / "custom_components/pymc_repeater"
NS = runpy.run_path(str(COMPONENT / "medre_status.py"))
parse = NS["parse_runtime"]


def response(now):
    return {"id": "medre.bridge", "exists": True, "path": "SECRET",
            "runtime": {"schema": NS["SCHEMA"], "process_id": 123, "updated_at": now, "phase": "running",
                        "health_scope": "retained", "runtime_health": "degraded",
                        "health_observed_at": now - 900, "adapters_started": 2,
                        "startup_routes_registered": 4, "startup_routes_blocked": 1,
                        "secret": "SECRET"}}


class MedreStatusTests(unittest.IsolatedAsyncioTestCase):
    def test_fresh_heartbeat_retains_old_observation_time_without_secrets(self):
        result = parse(response(1000), expected_pid=123, now=1000)
        self.assertEqual(result["health_observed_at"], 100)
        self.assertEqual(result["runtime_health"], "degraded")
        self.assertNotIn("SECRET", repr(result))

    def test_stale_malformed_or_future_observation_is_unavailable(self):
        for changes in ({"updated_at": 969}, {"updated_at": 1006}, {"updated_at": True},
                        {"updated_at": float("nan")}, {"updated_at": 10**500},
                        {"schema": "other"}, {"process_id": 124}, {"process_id": True}, {"phase": []}, {"runtime_health": {}},
                        {"adapters_started": True}, {"startup_routes_blocked": 5},
                        {"startup_routes_registered": None},
                        {"health_observed_at": 1006}, {"phase": "stopped"}):
            with self.subTest(changes=changes):
                payload = response(1000)
                payload["runtime"].update(changes)
                self.assertIsNone(parse(payload, expected_pid=123, now=1000))
        self.assertIsNone(parse(dict(response(1000), id="other"), expected_pid=123, now=1000))

    def test_known_registered_count_with_unknown_blocked_count_is_accepted(self):
        # The runtime status schema makes each route count independently
        # nullable: while startup readiness has not been computed, the
        # heartbeat reports a known registered count with a null blocked
        # count instead of dropping both.
        payload = response(1000)
        payload["runtime"].update({"phase": "starting", "health_scope": "unavailable",
                                   "runtime_health": None, "health_observed_at": None,
                                   "adapters_started": None,
                                   "startup_routes_registered": 4,
                                   "startup_routes_blocked": None})
        result = parse(payload, expected_pid=123, now=1000)
        self.assertEqual(result["phase"], "starting")
        self.assertEqual(result["startup_routes_registered"], 4)
        self.assertIsNone(result["startup_routes_blocked"])

    def test_startup_route_counts_are_retained_outside_the_running_phase(self):
        payload = response(1000)
        payload["runtime"].update({"phase": "stopped", "health_scope": "unavailable",
                                   "runtime_health": None, "health_observed_at": None,
                                   "adapters_started": None})
        result = parse(payload, expected_pid=123, now=1000)
        self.assertEqual(result["phase"], "stopped")
        self.assertEqual(result["startup_routes_registered"], 4)
        self.assertEqual(result["startup_routes_blocked"], 1)
        self.assertIsNone(result["adapters_started"])

    def test_non_running_phase_with_running_only_facts_is_rejected(self):
        for changes in ({"phase": "stopping"},
                        {"phase": "stopping", "health_scope": "unavailable",
                         "runtime_health": None, "health_observed_at": None}):
            with self.subTest(changes=changes):
                payload = response(1000)
                payload["runtime"].update(changes)
                self.assertIsNone(parse(payload, expected_pid=123, now=1000))

    async def test_bounded_optional_read_only_for_one_running_medre(self):
        ns = client_namespace()
        client = object.__new__(ns["PyMCRepeaterApiClient"])
        calls = []
        inventory = [{"id": "medre.bridge", "enabled": True, "state": "RUNNING", "pid": 123}]
        runtime_failure = False

        async def request(method, path, **kwargs):
            calls.append((path, kwargs))
            if path == "/api/plugins/":
                return {"plugins": inventory}
            if runtime_failure:
                raise ns["PyMCRepeaterApiError"]("SECRET")
            return {"success": True, "data": response(time.time())}

        client._async_request_json = request
        result = await client.async_get_plugin_summary()
        self.assertEqual(result["medre_status"]["runtime_health"], "degraded")
        self.assertNotIn("SECRET", repr(result))
        self.assertEqual(calls[1][0], "/api/plugins/runtime")
        self.assertEqual(calls[1][1]["max_response_bytes"], 16384)
        self.assertEqual(calls[1][1]["timeout_seconds"], 5)
        runtime_failure = True
        result = await client.async_get_plugin_summary()
        self.assertNotIn("medre_status", result)
        self.assertEqual(result["running"], 1)
        for replacement in ([dict(inventory[0], state="STOPPED")], inventory * 2, []):
            inventory = replacement
            calls.clear()
            result = await client.async_get_plugin_summary()
            self.assertNotIn("medre_status", result)
            self.assertEqual(len(calls), 1)

    async def test_runtime_endpoint_error_body_stays_within_the_byte_budget(self):
        from types import SimpleNamespace
        ns = client_namespace()
        ns["URL"] = SimpleNamespace(build=lambda **kw: "http://example.invalid:8000")
        ns["ClientError"] = type("ClientError", (Exception,), {})
        ns["ClientTimeout"] = lambda **kw: SimpleNamespace(**kw)
        client = object.__new__(ns["PyMCRepeaterApiClient"])
        client.host, client.port, client.api_token = "example.invalid", 8000, "example-token"
        calls = []

        class Content:
            def __init__(self, body):
                self.body, self.read_bytes = body, 0
            async def read(self, maximum):
                chunk, self.body = self.body[:maximum], self.body[maximum:]
                self.read_bytes += len(chunk)
                return chunk

        class Response:
            status = 200
            def __init__(self, body=b"", payload=None):
                self.content = Content(body)
                self.payload = payload or {}
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                self.closed = True
            async def json(self, **kwargs):
                return self.payload
            async def text(self):
                raise AssertionError("unbounded error body read")

        listing = Response(payload={"plugins": [
            {"id": "medre.bridge", "enabled": True, "state": "RUNNING", "pid": 123}]})
        failures = []

        def request(method, url, **kwargs):
            calls.append((method, url, kwargs))
            if url.endswith("/api/plugins/runtime"):
                failure = Response(body=b"x" * (500 * 1024))
                failure.status = 500
                failures.append(failure)
                return failure
            return listing

        client._session = SimpleNamespace(request=request)
        result = await client.async_get_plugin_summary()
        self.assertNotIn("medre_status", result)
        self.assertEqual(result["running"], 1)
        self.assertTrue(any(call[1].endswith("/api/plugins/runtime") for call in calls))
        self.assertLessEqual(failures[0].content.read_bytes, 16 * 1024)
        # The retained error detail stays bounded by the same byte budget.
        with self.assertRaises(ns["PyMCRepeaterApiError"]) as caught:
            await client._async_request_json("GET", "/api/plugins/runtime",
                auth="api_token", max_response_bytes=16384, timeout_seconds=5)
        self.assertLessEqual(len(str(caught.exception)), 300)
        self.assertLessEqual(failures[-1].content.read_bytes, 16 * 1024)

    async def test_auth_failure_still_surfaces(self):
        ns = client_namespace()
        client = object.__new__(ns["PyMCRepeaterApiClient"])

        async def request(method, path, **kwargs):
            if path == "/api/plugins/":
                return {"plugins": [{"id": "medre.bridge", "enabled": True, "state": "RUNNING", "pid": 123}]}
            raise ns["PyMCRepeaterAuthenticationError"]("denied")

        client._async_request_json = request
        with self.assertRaises(ns["PyMCRepeaterAuthenticationError"]):
            await client.async_get_plugin_summary()


def status_payload():
    return {"success": True, "data": response(time.time())}


class BoundedContent:
    def __init__(self, body):
        self.body, self.read_bytes = body, 0

    async def read(self, maximum):
        chunk, self.body = self.body[:maximum], self.body[maximum:]
        self.read_bytes += len(chunk)
        return chunk


class BoundedResponse:
    status = 200

    def __init__(self, body=b"", payload=None, headers=None):
        if payload is not None:
            body = json.dumps(payload).encode()
        self.content = BoundedContent(body)
        self.payload = payload or {}
        self.headers = headers or {}
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True

    async def json(self, **kwargs):
        return self.payload


def session_client(ns, calls, responses):
    """A client running the real _async_request_json over a scripted session."""
    from types import SimpleNamespace
    ns["URL"] = SimpleNamespace(build=lambda **kw: "http://example.invalid:8000")
    ns["ClientError"] = type("ClientError", (Exception,), {})
    ns["ClientTimeout"] = lambda **kw: SimpleNamespace(**kw)
    client = object.__new__(ns["PyMCRepeaterApiClient"])
    client.host, client.port, client.api_token = "example.invalid", 8000, "example-token"

    def request(method, url, **kwargs):
        calls.append((method, url, kwargs))
        return responses.pop(0)

    client._session = SimpleNamespace(request=request)
    return client


class MedreStatusTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_runtime_status_request_is_issued_without_following_redirects(self):
        ns = client_namespace()
        calls = []
        client = session_client(ns, calls, [BoundedResponse(payload=status_payload())])
        status = await client._async_get_medre_status(123)
        self.assertEqual(status["runtime_health"], "degraded")
        self.assertEqual(len(calls), 1)
        self.assertIs(calls[0][2].get("allow_redirects"), False)
        self.assertNotIn("SECRET", repr(status))

    async def test_runtime_status_redirect_is_refused_without_following_it(self):
        ns = client_namespace()
        calls = []
        location = {"Location": "http://other.invalid:9000/api/plugins/runtime?id=medre.bridge&secret=1"}
        redirect = BoundedResponse(payload=status_payload(), headers=location)
        redirect.status = 302
        client = session_client(ns, calls, [redirect, redirect])
        result = await client._async_get_medre_status(123)
        self.assertIsNone(result)
        self.assertEqual(len(calls), 1)
        self.assertIs(calls[0][2].get("allow_redirects"), False)
        with self.assertRaises(ns["PyMCRepeaterApiError"]) as caught:
            await client._async_request_json("GET", "/api/plugins/runtime",
                params={"id": "medre.bridge"}, auth="api_token",
                max_response_bytes=16384, timeout_seconds=5, follow_redirects=False)
        message = str(caught.exception)
        self.assertNotIn("Location", message)
        self.assertNotIn("other.invalid", message)
        self.assertNotIn("secret", message)

    async def test_existing_requests_keep_the_default_redirect_behaviour(self):
        ns = client_namespace()
        calls = []
        listing = BoundedResponse(payload={"success": True, "plugins": [
            {"id": "example", "enabled": True, "state": "RUNNING"}]})
        client = session_client(ns, calls, [listing])
        result = await client.async_get_plugin_summary()
        self.assertEqual(result["running"], 1)
        self.assertEqual(calls[0][0], "GET")
        self.assertTrue(calls[0][1].endswith("/api/plugins/"))
        self.assertEqual(len(calls), 1)
        self.assertNotIn("allow_redirects", calls[0][2])
