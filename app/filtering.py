"""Declarative per-subscription payload filters.

A filter is a JSON tree. Leaf conditions test a dot-separated path into the
event payload; combinators compose conditions::

    {"eq":     {"path": "a.b", "value": <json>}}
    {"in":     {"path": "a.b", "values": [<json>, ...]}}
    {"exists": {"path": "a.b"}}
    {"all":  [<condition>, ...]}
    {"any":  [<condition>, ...]}
    {"not":  <condition>}

Validation rejects unknown operators, empty ``all``/``any`` combinations and
trees deeper than ``MAX_FILTER_DEPTH`` (a leaf is depth 1, each combinator
adds one level). A subscription without a filter matches every event.
"""

from __future__ import annotations

from typing import Any

MAX_FILTER_DEPTH = 5

_LEAF_OPS = ("eq", "in", "exists")
_COMBINATORS = ("all", "any", "not")

# Sentinel for "path does not resolve", kept distinct from a present null.
_MISSING = object()


class FilterValidationError(ValueError):
    """Raised when a subscription filter violates the DSL rules."""


# ------------------------------------------------------------------ validation

def validate_filter(node: Any, depth: int = 1) -> None:
    """Raise FilterValidationError unless ``node`` is a well-formed filter."""
    if depth > MAX_FILTER_DEPTH:
        raise FilterValidationError(
            f"filter nesting exceeds the maximum of {MAX_FILTER_DEPTH} levels"
        )
    if not isinstance(node, dict) or len(node) != 1:
        raise FilterValidationError(
            "each condition must be an object with exactly one operator"
        )
    op, arg = next(iter(node.items()))
    if op in _COMBINATORS:
        _validate_combinator(op, arg, depth)
    elif op in _LEAF_OPS:
        _validate_leaf(op, arg)
    else:
        raise FilterValidationError(f"unknown filter operator {op!r}")


def _validate_combinator(op: str, arg: Any, depth: int) -> None:
    if op in ("all", "any"):
        if not isinstance(arg, list) or not arg:
            raise FilterValidationError(
                f"{op!r} requires a non-empty list of conditions"
            )
        for child in arg:
            validate_filter(child, depth + 1)
    else:  # not
        if not isinstance(arg, dict):
            raise FilterValidationError("'not' requires a single condition object")
        validate_filter(arg, depth + 1)


def _validate_leaf(op: str, arg: Any) -> None:
    if not isinstance(arg, dict):
        raise FilterValidationError(f"{op!r} requires an object argument")
    required = {
        "eq": {"path", "value"},
        "in": {"path", "values"},
        "exists": {"path"},
    }[op]
    if set(arg.keys()) != required:
        raise FilterValidationError(
            f"{op!r} requires exactly the keys {sorted(required)}"
        )
    _validate_path(arg["path"])
    if op == "in":
        values = arg["values"]
        if not isinstance(values, list) or not values:
            raise FilterValidationError("'in' requires a non-empty 'values' list")


def _validate_path(path: Any) -> None:
    if not isinstance(path, str) or not path:
        raise FilterValidationError("'path' must be a non-empty string")
    if any(not segment for segment in path.split(".")):
        raise FilterValidationError(f"invalid dot path {path!r}")


# ------------------------------------------------------------------- matching

def matches(spec: dict | None, payload: Any) -> bool:
    """Evaluate a validated filter against a decoded JSON payload.

    ``None`` (subscription created without a filter) matches everything.
    """
    if spec is None:
        return True
    op, arg = next(iter(spec.items()))
    if op == "all":
        return all(matches(child, payload) for child in arg)
    if op == "any":
        return any(matches(child, payload) for child in arg)
    if op == "not":
        return not matches(arg, payload)
    value = _resolve_path(payload, arg["path"])
    if value is _MISSING:
        return False
    if op == "exists":
        return True
    if op == "eq":
        return _json_equal(value, arg["value"])
    return any(_json_equal(value, item) for item in arg["values"])  # in


def _resolve_path(payload: Any, path: str) -> Any:
    node = payload
    for segment in path.split("."):
        if isinstance(node, dict) and segment in node:
            node = node[segment]
        else:
            return _MISSING
    return node


def _json_equal(a: Any, b: Any) -> bool:
    """JSON-aware equality: keep bool distinct from 0/1, allow int/float mix."""
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool) and a == b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return a == b
    if type(a) is not type(b):
        return False
    return a == b
