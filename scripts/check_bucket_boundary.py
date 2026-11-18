"""Live Q1 pagination check around a populated hot-partition bucket boundary."""
import datetime as dt
import json
import urllib.parse
import urllib.request

BASE = "http://localhost:8080"
def get(path, params=None):
    url = BASE + path + ("?" + urllib.parse.urlencode(params) if params else "")
    request = urllib.request.Request(url, headers={"Person-Id": "100000"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return json.load(response)

catalog = get("/api/admin/cassandra/buckets", {"stream": "type:HOT_PARTITION_DEMO"})

if isinstance(catalog, dict):
    raise AssertionError(f"Unexpected catalog: {catalog}")
catalog = sorted(catalog, key=lambda b: b["bucket"])
assert len(catalog) >= 2, "Run hot-partition fill first"

boundary = dt.datetime.fromisoformat(catalog[-1]["firstEventAt"].replace("Z", "+00:00"))
params = {"type": "HOT_PARTITION_DEMO", "from": (boundary - dt.timedelta(milliseconds=10)).isoformat(),
          "to": (boundary + dt.timedelta(milliseconds=10)).isoformat(), "size": 500, "consistency": "QUORUM"}
whole = get("/api/events/by-type", dict(params, order="asc"))
assert whole.get("nextPageToken") is None, "Narrow the period: reference must fit on one page"
reference = [e["eventId"] for e in whole["items"]]
assert len({e["bucket"] for e in whole["items"]}) >= 2, "Period must contain both sides of the boundary"
checks = []
for order in ("asc", "desc"):
    seen = []
    query = dict(params, order=order, size=7)
    while True:
        page = get("/api/events/by-type", query)
        seen.extend(e["eventId"] for e in page["items"])
        token = page.get("nextPageToken")
        if not token:
            break
        query["pageToken"] = token
    assert seen == (reference if order == "asc" else reference[::-1]), order
    assert len(seen) == len(set(seen)), "Duplicates"
    checks.append({"order": order, "events": len(seen), "passed": True})
print(json.dumps({"boundary": boundary.isoformat(), "period": params,
                  "buckets": sorted({e["bucket"] for e in whole["items"]}), "checks": checks}, ensure_ascii=False, indent=2))
