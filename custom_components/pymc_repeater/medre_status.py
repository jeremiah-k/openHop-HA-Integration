"""Allowlisted MEDRE plugin heartbeat; never retain raw plugin runtime data."""

from __future__ import annotations

import math
import time
from typing import Any

PLUGIN_ID = "medre.bridge"
SCHEMA = "medre.runtime-status.v1"
MAX_AGE_SECONDS = 30
MAX_FUTURE_SECONDS = 5
PHASES = {"starting", "running", "stopping", "stopped", "failed"}
HEALTH = {"healthy", "degraded", "failed"}
COUNTS = ("adapters_started", "startup_routes_registered", "startup_routes_blocked")


def _timestamp(value: Any) -> bool:
    # Bound before conversion to float so arbitrarily large JSON ints cannot
    # raise OverflowError. Reject booleans, NaN and infinities.
    return type(value) in (int, float) and 0 < value < 253402300800 and math.isfinite(value)


def fresh(status: dict[str, Any], *, now: float | None = None,
          max_age: float = MAX_AGE_SECONDS) -> bool:
    """Report whether the heartbeat is recent enough for the reader.

    ``max_age`` is the caller's staleness budget: a value cached between
    coordinator polls must not be judged against the source heartbeat
    ceiling alone.
    """
    stamp = status.get("updated_at")
    if not _timestamp(stamp):
        return False
    age = (time.time() if now is None else now) - stamp
    return -MAX_FUTURE_SECONDS <= age <= max_age


def parse_runtime(payload: Any, *, expected_pid: int, now: float | None = None) -> dict[str, Any] | None:
    """Return only documented scalar observations for the exact plugin ID."""
    if not isinstance(payload, dict) or payload.get("id") != PLUGIN_ID or payload.get("exists") is not True:
        return None
    row = payload.get("runtime")
    if not isinstance(row, dict) or row.get("schema") != SCHEMA or not fresh(row, now=now):
        return None
    pid = row.get("process_id")
    if type(pid) is not int or pid <= 0 or pid != expected_pid:
        return None
    phase, scope, health = row.get("phase"), row.get("health_scope"), row.get("runtime_health")
    if not isinstance(phase, str) or phase not in PHASES:
        return None
    if not isinstance(scope, str) or scope not in {"unavailable", "startup", "retained"}:
        return None
    observed = row.get("health_observed_at")
    if scope == "unavailable":
        if health is not None or observed is not None:
            return None
    elif not isinstance(health, str) or health not in HEALTH:
        return None
    elif scope == "retained":
        if not _timestamp(observed) or observed > row["updated_at"] + MAX_FUTURE_SECONDS:
            return None
    elif observed is not None:
        return None
    result = {"updated_at": row["updated_at"], "phase": phase, "health_scope": scope,
              "runtime_health": health, "health_observed_at": observed}
    for key in COUNTS:
        count = row.get(key)
        if count is not None and (type(count) is not int or not 0 <= count <= 2**31 - 1):
            return None
        result[key] = count
    # Startup route counts are startup-derived facts and are accepted in
    # every phase; adapters_started, the health scope and runtime health
    # are running-only observations.
    if phase != "running" and (scope != "unavailable" or result["adapters_started"] is not None):
        return None
    registered, blocked = result["startup_routes_registered"], result["startup_routes_blocked"]
    # The status schema makes each route count independently nullable
    # ("unknown values are null"): the runtime reports a known registered
    # count with a still-unknown blocked count while startup readiness has
    # not been computed. A known blocked count without a registered count
    # stays refused: blocked routes are a subset of the registered ones.
    if blocked is not None and (registered is None or blocked > registered):
        return None
    return result
