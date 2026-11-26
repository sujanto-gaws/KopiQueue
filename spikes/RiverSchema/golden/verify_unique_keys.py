"""Spike 0.8: clean-room model of River's unique key, derived only from black-box outputs.

Checks every vector in river-v0.51.2-unique-keys.jsonl. Usage: python -I verify_unique_keys.py <vectors.jsonl>
The JSON canonicalisation variants are tried in turn; the report says which one matches each vector.
"""
import datetime as dt
import hashlib
import json
import sys

STATES = ["available", "cancelled", "completed", "discarded", "pending", "retryable", "running", "scheduled"]


def states_bits(states):
    # Observed: bit string position i (left to right) == state STATES[7 - i]; i.e. "scheduled" is the leftmost bit.
    s = set(states or ["available", "completed", "pending", "retryable", "running", "scheduled"])
    return "".join("1" if STATES[7 - i] in s else "0" for i in range(8))


UNIQUE_FIELDS = {"golden_tagged": {"region", "customer", "omit"}}  # fields tagged `river:"unique"` in the harness

_dec = json.JSONDecoder()
WS = " \t\r\n"


def top_level_raw(text):
    """Split a JSON object into (key, raw value text) pairs without re-encoding the values."""
    i, n, out = text.index("{") + 1, len(text), []
    while True:
        while text[i] in WS + ",": i += 1
        if text[i] == "}": return out
        key, i = _dec.raw_decode(text, i)
        while text[i] in WS + ":": i += 1
        _, j = _dec.raw_decode(text, i)
        out.append((key, text[i:j]))
        i = j


def canon_args(kind, args_json):
    """Model: keep only unique-tagged fields (if the type has any), sort top-level keys, copy values verbatim."""
    pairs = top_level_raw(args_json)
    if kind in UNIQUE_FIELDS:
        pairs = [(k, v) for k, v in pairs if k in UNIQUE_FIELDS[kind]]
    pairs.sort(key=lambda kv: kv[0].encode("utf-8"))
    return "{" + ",".join(json.dumps(k, ensure_ascii=False) + ":" + v for k, v in pairs) + "}"


def unique_key(v):
    o = v["unique_opts"]
    parts = []
    if not o.get("ExcludeKind"):
        parts.append("&kind=" + v["kind"])
    if o.get("ByArgs"):
        parts.append("&args=" + canon_args(v["kind"], v["args_json"]))
    if o.get("ByPeriod"):
        ns = o["ByPeriod"]
        created = dt.datetime.fromisoformat(v["created_at_utc"].replace("Z", "+00:00"))
        epoch_ns = int(created.timestamp()) * 1_000_000_000
        trunc = dt.datetime.fromtimestamp(epoch_ns // ns * ns / 1e9, dt.timezone.utc)
        parts.append("&period=" + trunc.strftime("%Y-%m-%dT%H:%M:%SZ"))
    if o.get("ByQueue"):
        parts.append("&queue=" + v["queue"])
    return hashlib.sha256("".join(parts).encode()).hexdigest(), "".join(parts)


ok = bad = 0
for line in open(sys.argv[1], encoding="utf-8"):
    v = json.loads(line)
    key, pre = unique_key(v)
    hits = ["match"] if key == v["unique_key_hex"] else []
    st_ok = states_bits(v["unique_opts"].get("ByState")) == v["unique_states"]
    status = "OK " if hits and st_ok else "BAD"
    ok += status == "OK "
    bad += status == "BAD"
    print(f"{status} {v['name']:<32} key:{','.join(hits) or 'MISMATCH'} states:{'ok' if st_ok else 'MISMATCH'}  {pre[:90]}")
print(f"\n{ok} ok, {bad} bad")
